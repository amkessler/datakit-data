import os
import logging
import mimetypes
import fnmatch
import time
import threading
import json
import hashlib
import tempfile
import zipfile
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import NullHandler

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .sync_markers import SyncMarkers


logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

# Metadata captured per remote object from a list_objects_v2 listing. The ETag identifies the
# object's content; it is what we compare against the recorded .synced marker to detect whether
# the object changed on S3 since our last sync.
S3ObjectInfo = namedtuple('S3ObjectInfo', ['etag'])

# Local filesystem issue found before transfer starts.
LocalFileIssue = namedtuple('LocalFileIssue', ['path', 'message'])

# A per-file push decision made after preflight: skipped=True means the sync marker says
# the local file is already current; otherwise the file should be uploaded or reported in dryrun.
PushDecision = namedtuple('PushDecision', ['rel_path', 'local_path', 'key', 'skipped'])

# The result of S3.compare: sorted lists of rel_paths bucketed by how the local copy, the live
# remote object, and the recorded .synced marker disagree. 'differ' holds files present on both
# sides whose difference cannot be attributed for lack of a usable sync record.
SyncComparison = namedtuple(
    'SyncComparison', ['only_local', 'only_s3', 'changed_local', 'changed_s3', 'conflict', 'differ']
)

EMPTY_PATH_DELETE_MSG = (
    "\n*** Refusing --delete with an empty s3_path: this would scan and delete "
    "across the entire bucket. Set s3_path in the project config. ***\n"
)

FILTERED_PATH_DELETE_MSG = (
    "\n*** Refusing --delete with push filters: this would compare a filtered local "
    "file set against the full S3 prefix. Run delete without --path/--include/--exclude. ***\n"
)

ARCHIVE_REQUIRES_ONE_PATH_MSG = "\n*** Archive mode requires exactly one --path value. ***\n"
ARCHIVE_PRUNE_REQUIRES_ARCHIVE_MSG = "\n*** --prune-individuals requires --archive. ***\n"
ARCHIVE_FILTERS_UNSUPPORTED_MSG = "\n*** Archive mode does not support --include or --exclude. ***\n"

ARCHIVE_EXT = '.zip'
ARCHIVE_MANIFEST_EXT = '.manifest.json'
ARCHIVE_METADATA_FILENAME = 'datakit-data-archives.json'


def _normalize_rel_path(path):
    return path.replace('\\', '/').strip('/')


def _strip_data_prefix(path):
    rel_path = _normalize_rel_path(path)
    if rel_path == 'data':
        return ''
    if rel_path.startswith('data/'):
        return rel_path[len('data/'):]
    return rel_path


def _normalize_filter_path(data_dir, path):
    if os.path.isabs(path):
        rel_path = os.path.relpath(path, data_dir)
    else:
        rel_path = path
    rel_path = _normalize_rel_path(os.path.normpath(_strip_data_prefix(rel_path)))
    if rel_path == '.':
        return ''
    if rel_path == '..' or rel_path.startswith('../'):
        return None
    return rel_path


def _matches_any(rel_path, patterns):
    return any(fnmatch.fnmatchcase(rel_path, pattern) for pattern in patterns)


def _selected_by_patterns(rel_path, include_patterns, exclude_patterns):
    if include_patterns and not _matches_any(rel_path, include_patterns):
        return False
    if exclude_patterns and _matches_any(rel_path, exclude_patterns):
        return False
    return True


def _candidate_roots(data_dir, paths):
    if not paths:
        return [data_dir]
    roots = []
    for path in paths:
        rel_path = _normalize_filter_path(data_dir, path)
        if rel_path is None:
            continue
        roots.append(os.path.join(data_dir, *rel_path.split('/')) if rel_path else data_dir)
    return roots


def _archive_rel_paths(data_dir, path):
    rel_path = _normalize_filter_path(data_dir, path)
    if rel_path is None or not rel_path:
        return None, None
    archive_rel = rel_path + ARCHIVE_EXT
    manifest_rel = rel_path + ARCHIVE_MANIFEST_EXT
    return archive_rel, manifest_rel


def _archive_metadata_dir(sync_status_dir):
    return sync_status_dir or '.sync_status'


def _archive_metadata_path(sync_status_dir):
    return os.path.join(_archive_metadata_dir(sync_status_dir), ARCHIVE_METADATA_FILENAME)


def _archive_metadata_rel_paths(data_dir, sync_status_dir):
    metadata_path = os.path.abspath(_archive_metadata_path(sync_status_dir))
    data_root = os.path.abspath(data_dir)
    try:
        if os.path.commonpath([data_root, metadata_path]) != data_root:
            return set()
    except ValueError:
        return set()
    return {_normalize_rel_path(os.path.relpath(metadata_path, data_root))}


def read_archive_paths(sync_status_dir):
    metadata_path = _archive_metadata_path(sync_status_dir)
    if not os.path.exists(metadata_path):
        return []
    with open(metadata_path) as f:
        data = json.load(f)
    return sorted(set(data.get('archive_paths', [])))


def write_archive_paths(sync_status_dir, archive_paths):
    metadata_path = _archive_metadata_path(sync_status_dir)
    os.makedirs(os.path.dirname(os.path.abspath(metadata_path)), exist_ok=True)
    data = {
        'version': 1,
        'archive_paths': sorted(set(archive_paths)),
    }
    with open(metadata_path, 'w') as f:
        json.dump(data, f, indent=2, sort_keys=True)


def register_archive_path(sync_status_dir, archive_path):
    archive_paths = read_archive_paths(sync_status_dir)
    if archive_path not in archive_paths:
        archive_paths.append(archive_path)
        write_archive_paths(sync_status_dir, archive_paths)


def _is_under_archive_path(rel_path, archive_path):
    return rel_path == archive_path or rel_path.startswith(archive_path.rstrip('/') + '/')


def _is_archive_managed_remote_path(rel_path, archive_path):
    archive_path = archive_path.rstrip('/')
    return (
        _is_under_archive_path(rel_path, archive_path) or
        rel_path == archive_path + ARCHIVE_EXT or
        rel_path == archive_path + ARCHIVE_MANIFEST_EXT
    )


def _normalize_archive_manifest_path(path):
    if not isinstance(path, str):
        return None
    rel_path = _normalize_rel_path(os.path.normpath(path.replace('\\', '/')))
    if not rel_path or rel_path == '.' or rel_path == '..' or rel_path.startswith('../'):
        return None
    return rel_path


def _filter_archive_managed_files(local_files, archive_paths):
    if not archive_paths:
        return local_files
    return {
        rel_path: local_path
        for rel_path, local_path in local_files.items()
        if not any(_is_under_archive_path(rel_path, archive_path) for archive_path in archive_paths)
    }


def _normalized_archive_paths(archive_paths):
    return [
        rel_path
        for rel_path in (_normalize_archive_manifest_path(path) for path in archive_paths or [])
        if rel_path is not None
    ]


def _join_rel_path(root_rel, name):
    return _normalize_rel_path(os.path.join(root_rel, name)) if root_rel else _normalize_rel_path(name)


def list_local_files(
    data_dir, paths=None, include_patterns=None, exclude_patterns=None, ignored_rel_paths=None,
    archive_paths_to_skip=None
):
    # Map of rel_path -> full path for every data file under data_dir, excluding .synced
    # markers (which live alongside the data when sync_status_location is data/). The key is
    # used to build/compare S3 keys, which always use '/'; normalize the OS separator so keys
    # generated on Windows match remote keys, while the value stays OS-native for filesystem
    # operations.
    paths = paths or []
    include_patterns = [_strip_data_prefix(pattern) for pattern in include_patterns or []]
    exclude_patterns = [_strip_data_prefix(pattern) for pattern in exclude_patterns or []]
    ignored_rel_paths = set(ignored_rel_paths or [])
    archive_paths_to_skip = _normalized_archive_paths(archive_paths_to_skip)
    files = {}
    for root_path in _candidate_roots(data_dir, paths):
        if os.path.isfile(root_path) or os.path.islink(root_path):
            root = os.path.dirname(root_path)
            rel_path = os.path.relpath(root_path, data_dir).replace(os.sep, '/')
            if any(_is_under_archive_path(rel_path, archive_path) for archive_path in archive_paths_to_skip):
                continue
            candidates = [(root, [], [os.path.basename(root_path)])]
        elif os.path.isdir(root_path):
            candidates = os.walk(root_path)
        else:
            continue
        for root, dirnames, filenames in candidates:
            root_rel = os.path.relpath(root, data_dir).replace(os.sep, '/')
            if root_rel == '.':
                root_rel = ''
            if any(_is_under_archive_path(root_rel, archive_path) for archive_path in archive_paths_to_skip):
                dirnames[:] = []
                continue
            if archive_paths_to_skip:
                dirnames[:] = [
                    dirname for dirname in dirnames
                    if not any(
                        _is_under_archive_path(_join_rel_path(root_rel, dirname), archive_path)
                        for archive_path in archive_paths_to_skip
                    )
                ]
            for filename in filenames:
                if filename.endswith(SyncMarkers.SUFFIX):
                    continue
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, data_dir).replace(os.sep, '/')
                if rel_path in ignored_rel_paths:
                    continue
                if not _selected_by_patterns(rel_path, include_patterns, exclude_patterns):
                    continue
                files[rel_path] = full_path
    return files


def list_data_files(
    data_dir, sync_status_dir=None, paths=None, include_patterns=None, exclude_patterns=None,
    skip_archive_managed=False
):
    archive_paths = read_archive_paths(sync_status_dir) if skip_archive_managed else []
    local_files = list_local_files(
        data_dir,
        paths=paths,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        ignored_rel_paths=_archive_metadata_rel_paths(data_dir, sync_status_dir),
        archive_paths_to_skip=archive_paths,
    )
    return local_files


def validate_local_file(local_path):
    try:
        if os.path.islink(local_path) and not os.path.exists(local_path):
            return LocalFileIssue(local_path, 'broken symlink')
        if os.path.exists(local_path) and not os.path.isfile(local_path):
            return LocalFileIssue(local_path, 'not a regular file')
        if os.path.exists(local_path):
            with open(local_path, 'rb'):
                pass
    except OSError as e:
        return LocalFileIssue(local_path, str(e))
    return None


def validate_local_files(local_files, progress_callback=None, jobs=1):
    issues = []
    sorted_paths = [local_path for _, local_path in sorted(local_files.items())]
    total = len(sorted_paths)
    jobs = max(1, int(jobs or 1))
    if jobs == 1:
        for index, local_path in enumerate(sorted_paths, start=1):
            issue = validate_local_file(local_path)
            if issue:
                issues.append(issue)
            if progress_callback:
                progress_callback(index, total, len(issues))
        return issues
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = [executor.submit(validate_local_file, local_path) for local_path in sorted_paths]
        for index, future in enumerate(as_completed(futures), start=1):
            issue = future.result()
            if issue:
                issues.append(issue)
            if progress_callback:
                progress_callback(index, total, len(issues))
    return issues


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


class _ArchiveUploadProgress:
    def __init__(self, key, total_bytes, interval_seconds, interval_bytes):
        self.key = key
        self.total_bytes = total_bytes
        self.interval_seconds = interval_seconds
        self.interval_bytes = interval_bytes
        self.transferred = 0
        self.started = time.monotonic()
        self.last_logged = self.started
        self.last_logged_bytes = 0

    def __call__(self, bytes_amount):
        self.transferred += bytes_amount
        now = time.monotonic()
        should_log = (
            self.transferred >= self.total_bytes or
            (
                now - self.last_logged >= self.interval_seconds and
                self.transferred - self.last_logged_bytes >= self.interval_bytes
            )
        )
        if should_log:
            self._log(now)

    def finish(self):
        if self.transferred and self.transferred < self.total_bytes:
            self._log(time.monotonic())

    def _log(self, now):
        elapsed = max(now - self.started, 0.001)
        rate = self.transferred / elapsed
        percent = (self.transferred / self.total_bytes * 100) if self.total_bytes else 100
        logger.info(
            f"archive upload progress: {self.key} "
            f"{S3._format_bytes(self.transferred)}/{S3._format_bytes(self.total_bytes)} "
            f"{percent:.1f}% rate={S3._format_bytes(rate)}/s elapsed={elapsed:.1f}s"
        )
        self.last_logged = now
        self.last_logged_bytes = self.transferred


class S3:
    """A limited, human-friendly interface to S3."""

    # boto3's managed upload_file transparently switches from a single PutObject to a multipart
    # upload above this many bytes (its TransferConfig.multipart_threshold default). We mirror it:
    # files below the threshold take the put_object fast path, whose response carries the ETag, so
    # we avoid a follow-up head_object; larger files keep upload_file's multipart transfer (and its
    # part-level resilience) and we read the ETag back with head_object. Either way the ETag we
    # record is the one S3 itself reports, so it stays comparable to a later head/list probe.
    MULTIPART_THRESHOLD = 8 * 1024 * 1024
    PUSH_PROGRESS_INTERVAL = 1000
    ARCHIVE_UPLOAD_PROGRESS_SECONDS = 5
    ARCHIVE_UPLOAD_PROGRESS_BYTES = 64 * 1024 * 1024

    def __init__(self, aws_user_profile, s3_bucket):
        self.user_profile = aws_user_profile
        self.bucket = s3_bucket

    def push(
        self, data_dir, s3_path='', extra_flags=None, sync_status_dir=None,
        paths=None, include_patterns=None, exclude_patterns=None, jobs=1, archive=False,
        prune_individuals=False
    ):
        extra_flags = extra_flags or []
        paths = paths or []
        include_patterns = include_patterns or []
        exclude_patterns = exclude_patterns or []
        jobs = max(1, int(jobs or 1))
        dryrun = '--dryrun' in extra_flags or '--dry-run' in extra_flags
        delete = '--delete' in extra_flags
        force = '--force' in extra_flags
        verbose = '--verbose' in extra_flags
        prefix = self._normalize_prefix(s3_path)
        if delete and not prefix:
            logger.info(EMPTY_PATH_DELETE_MSG)
            return 1
        if delete and (paths or include_patterns or exclude_patterns):
            logger.info(FILTERED_PATH_DELETE_MSG)
            return 1
        if prune_individuals and not archive:
            logger.info(ARCHIVE_PRUNE_REQUIRES_ARCHIVE_MSG)
            return 1
        if archive and (include_patterns or exclude_patterns):
            logger.info(ARCHIVE_FILTERS_UNSUPPORTED_MSG)
            return 1
        if archive:
            return self.push_archive(data_dir, s3_path, paths, extra_flags, sync_status_dir, prune_individuals, jobs)
        markers = SyncMarkers(sync_status_dir)
        failures = 0
        logger.info("push discovery: scanning local files")
        archive_paths = read_archive_paths(sync_status_dir)
        if archive_paths:
            logger.info(f"push discovery: skipping archive-managed path(s): {', '.join(archive_paths)}")
        local_files = list_data_files(
            data_dir,
            sync_status_dir=sync_status_dir,
            paths=paths,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            skip_archive_managed=True,
        )
        started = time.monotonic()
        total = len(local_files)
        logger.info(f"push discovery: selected {total} file(s)")
        processed = 0
        uploaded = 0
        skipped = 0
        logger.info(f"push preflight: validating selected files with {jobs} worker(s)")
        preflight_issues = validate_local_files(
            local_files,
            progress_callback=self._log_preflight_progress,
            jobs=jobs,
        )
        if preflight_issues:
            for issue in preflight_issues:
                logger.info(f"preflight error: {issue.path}: {issue.message}")
            failures = len(preflight_issues)
            logger.info(f"{failures} file(s) failed preflight validation")
            self._log_push_summary(total, uploaded, skipped, failures, started)
            return failures
        logger.info("push preflight: ok")
        client = self._client() if jobs == 1 and not dryrun else None
        upload_items = []
        for decision in self._iter_push_decisions(local_files, markers, prefix, force, jobs):
            if decision.skipped:
                skipped += 1
                processed += 1
                if verbose:
                    logger.info(f"skipped: {decision.local_path}")
                self._log_push_progress(processed, total, uploaded, skipped, failures, started)
                continue
            if dryrun or verbose:
                logger.info(f"upload: {decision.local_path} to s3://{self.bucket}/{decision.key}")
            if jobs > 1 and not dryrun:
                upload_items.append((decision.rel_path, decision.local_path, decision.key))
            else:
                if not dryrun:
                    try:
                        self._upload_and_mark(client, decision.rel_path, decision.local_path, decision.key, markers)
                        uploaded += 1
                    except (ClientError, BotoCoreError, OSError) as e:
                        failures += 1
                        logger.info(f"\n*** Error ***\n{e}\n")
                else:
                    uploaded += 1
                processed += 1
                self._log_push_progress(processed, total, uploaded, skipped, failures, started)
        if upload_items:
            logger.info(f"push upload: uploading {len(upload_items)} file(s) with {jobs} worker(s)")
            thread_clients = threading.local()
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                futures = [
                    executor.submit(self._upload_with_thread_client, thread_clients, item, markers.enabled)
                    for item in upload_items
                ]
                for future in as_completed(futures):
                    try:
                        rel_path, etag = future.result()
                        if markers.enabled:
                            markers.write(rel_path, etag)
                        uploaded += 1
                    except (ClientError, BotoCoreError, OSError) as e:
                        failures += 1
                        logger.info(f"\n*** Error ***\n{e}\n")
                    processed += 1
                    self._log_push_progress(processed, total, uploaded, skipped, failures, started)
        if delete:
            if client is None:
                client = self._client()
            remote_keys = self._list_s3_keys(client, prefix)
            remote_rel = {k[len(prefix):] for k in remote_keys}
            to_delete = [
                prefix + rel_path
                for rel_path in sorted(remote_rel - set(local_files.keys()))
                if not any(_is_archive_managed_remote_path(rel_path, archive_path) for archive_path in archive_paths)
            ]
            for key in to_delete:
                logger.info(f"delete: s3://{self.bucket}/{key}")
            if not dryrun:
                failures += self._delete_keys(client, to_delete)
        self._log_push_summary(total, uploaded, skipped, failures, started)
        return failures

    def push_archive(
        self, data_dir, s3_path='', paths=None, extra_flags=None, sync_status_dir=None,
        prune_individuals=False, jobs=1
    ):
        extra_flags = extra_flags or []
        paths = paths or []
        jobs = max(1, int(jobs or 1))
        dryrun = '--dryrun' in extra_flags or '--dry-run' in extra_flags
        if len(paths) != 1:
            logger.info(ARCHIVE_REQUIRES_ONE_PATH_MSG)
            return 1
        archive_rel, manifest_rel = _archive_rel_paths(data_dir, paths[0])
        if archive_rel is None:
            logger.info(ARCHIVE_REQUIRES_ONE_PATH_MSG)
            return 1
        archive_root_rel = _normalize_filter_path(data_dir, paths[0])
        prefix = self._normalize_prefix(s3_path)
        archive_key = prefix + archive_rel
        manifest_key = prefix + manifest_rel
        archive_root = os.path.join(data_dir, *archive_root_rel.split('/'))
        local_files = list_data_files(data_dir, sync_status_dir=sync_status_dir, paths=paths)
        logger.info(f"archive push: selected {len(local_files)} file(s) under {paths[0]}")
        logger.info(f"archive preflight: validating selected files with {jobs} worker(s)")
        issues = validate_local_files(
            local_files,
            progress_callback=self._log_archive_preflight_progress,
            jobs=jobs,
        )
        if issues:
            for issue in issues:
                logger.info(f"preflight error: {issue.path}: {issue.message}")
            return len(issues)
        logger.info("archive preflight: ok")
        if dryrun:
            logger.info(f"archive build: would create {archive_rel} from {len(local_files)} file(s)")
            logger.info(f"archive upload: {archive_rel} to s3://{self.bucket}/{archive_key}")
            logger.info(f"archive upload: {manifest_rel} to s3://{self.bucket}/{manifest_key}")
            if prune_individuals:
                logger.info(
                    f"archive prune: would delete individual objects below "
                    f"s3://{self.bucket}/{prefix + archive_root_rel}/ after successful upload"
                )
            return 0
        markers = SyncMarkers(sync_status_dir)
        client = self._client()
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = os.path.join(tmpdir, os.path.basename(archive_rel))
            manifest_path = os.path.join(tmpdir, os.path.basename(manifest_rel))
            manifest = self._create_archive(data_dir, archive_root, local_files, archive_rel, archive_path)
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2, sort_keys=True)
            try:
                archive_etag = self._upload_archive_file(client, archive_path, archive_key, markers.enabled)
                manifest_etag = self._upload_archive_file(client, manifest_path, manifest_key, markers.enabled)
                if markers.enabled:
                    markers.write(archive_rel, archive_etag)
                    markers.write(manifest_rel, manifest_etag)
            except (ClientError, BotoCoreError, OSError) as e:
                logger.info(f"\n*** Error ***\n{e}\n")
                return 1
        register_archive_path(sync_status_dir, archive_root_rel)
        if prune_individuals:
            return self._prune_archive_individuals(client, prefix, archive_root_rel)
        return 0

    def _create_archive(self, data_dir, archive_root, local_files, archive_rel, archive_path):
        root_rel = os.path.relpath(archive_root, data_dir).replace(os.sep, '/')
        manifest_files = []
        total = len(local_files)
        started = time.monotonic()
        logger.info(f"archive build: creating {archive_rel} from {total} file(s)")
        with zipfile.ZipFile(archive_path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            for processed, (rel_path, local_path) in enumerate(sorted(local_files.items()), start=1):
                archive.write(local_path, arcname=rel_path)
                stat = os.stat(local_path)
                manifest_files.append({
                    'path': rel_path,
                    'size': stat.st_size,
                    'mtime': stat.st_mtime,
                })
                self._log_archive_build_progress(processed, total, archive_path, started)
        self._log_archive_build_complete(archive_path, started)
        return {
            'version': 1,
            'archive_type': 'zip',
            'archive_path': archive_rel,
            'archive_sha256': _sha256_file(archive_path),
            'root_path': root_rel,
            'files': manifest_files,
        }

    def _prune_archive_individuals(self, client, prefix, archive_root_rel):
        list_prefix = prefix + archive_root_rel.rstrip('/')
        remote_keys = self._list_s3_keys(client, list_prefix)
        to_delete = [
            key for key in sorted(remote_keys)
            if _is_under_archive_path(key[len(prefix):], archive_root_rel)
        ]
        logger.info(
            f"archive prune: deleting {len(to_delete)} individual object(s) below "
            f"s3://{self.bucket}/{prefix + archive_root_rel.rstrip('/')}/"
        )
        for key in to_delete:
            logger.info(f"delete: s3://{self.bucket}/{key}")
        return self._delete_keys(client, to_delete, progress_label='archive prune')

    def _iter_push_decisions(self, local_files, markers, prefix, force, jobs):
        items = sorted(local_files.items())
        if jobs == 1:
            for item in items:
                yield self._push_decision(markers, prefix, force, item)
            return
        logger.info(f"push decision: checking selected files with {jobs} worker(s)")
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = [
                executor.submit(self._push_decision, markers, prefix, force, item)
                for item in items
            ]
            for future in futures:
                yield future.result()

    def _push_decision(self, markers, prefix, force, item):
        rel_path, local_path = item
        key = prefix + rel_path
        skipped = not force and markers.is_fresh(rel_path, local_path)
        return PushDecision(rel_path, local_path, key, skipped)

    def _upload_and_mark(self, client, rel_path, local_path, key, markers):
        etag = self._upload(client, local_path, key, markers.enabled)
        if markers.enabled:
            markers.write(rel_path, etag)

    def _upload_with_thread_client(self, thread_clients, item, need_etag):
        rel_path, local_path, key = item
        if not hasattr(thread_clients, 'client'):
            thread_clients.client = self._client()
        etag = self._upload(thread_clients.client, local_path, key, need_etag)
        return rel_path, etag

    def _upload_archive_file(self, client, local_path, key, need_etag):
        logger.info(f"archive upload: {os.path.basename(local_path)} to s3://{self.bucket}/{key}")
        progress = self._archive_upload_progress_callback(local_path, key)
        try:
            return self._upload(client, local_path, key, need_etag, callback=progress)
        finally:
            progress.finish()

    def pull(
        self, data_dir, s3_path='', extra_flags=None, sync_status_dir=None,
        archive=False, paths=None, expand_archives=False
    ):
        extra_flags = extra_flags or []
        paths = paths or []
        dryrun = '--dryrun' in extra_flags or '--dry-run' in extra_flags
        delete = '--delete' in extra_flags
        force = '--force' in extra_flags
        prefix = self._normalize_prefix(s3_path)
        if delete and not prefix:
            logger.info(EMPTY_PATH_DELETE_MSG)
            return 1
        if archive:
            return self.pull_archive(data_dir, s3_path, paths, extra_flags, sync_status_dir)
        markers = SyncMarkers(sync_status_dir)
        client = self._client()
        failures = 0
        remote_objects = self._list_s3_objects(client, prefix)
        for rel_path in sorted(remote_objects):
            key = prefix + rel_path
            remote_etag = remote_objects[rel_path].etag
            local_path = os.path.join(data_dir, rel_path)
            if not force:
                marker_etag = markers.etag(rel_path)
                if marker_etag is not None and marker_etag == remote_etag and os.path.exists(local_path):
                    logger.info(f"skipped: s3://{self.bucket}/{key}")
                    continue
            logger.info(f"download: s3://{self.bucket}/{key} to {local_path}")
            if not dryrun:
                os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
                try:
                    client.download_file(self.bucket, key, local_path)
                    if markers.enabled:
                        markers.write(rel_path, remote_etag)
                except (ClientError, BotoCoreError) as e:
                    failures += 1
                    logger.info(f"\n*** Error ***\n{e}\n")
        if delete:
            local_files = list_data_files(data_dir, sync_status_dir=sync_status_dir, skip_archive_managed=True)
            remote_rel = set(remote_objects)
            archive_paths = read_archive_paths(sync_status_dir)
            for rel_path, local_path in sorted(local_files.items()):
                if any(_is_under_archive_path(rel_path, archive_path) for archive_path in archive_paths):
                    continue
                if rel_path not in remote_rel:
                    logger.info(f"delete: {local_path}")
                    if not dryrun:
                        try:
                            os.remove(local_path)
                        except OSError as e:
                            failures += 1
                            logger.info(f"\n*** Error ***\n{e}\n")
        if expand_archives:
            failures += self._expand_local_archives(data_dir, dryrun, sync_status_dir)
        return failures

    def pull_archive(self, data_dir, s3_path='', paths=None, extra_flags=None, sync_status_dir=None):
        extra_flags = extra_flags or []
        paths = paths or []
        dryrun = '--dryrun' in extra_flags or '--dry-run' in extra_flags
        if len(paths) != 1:
            logger.info(ARCHIVE_REQUIRES_ONE_PATH_MSG)
            return 1
        archive_rel, manifest_rel = _archive_rel_paths(data_dir, paths[0])
        if archive_rel is None:
            logger.info(ARCHIVE_REQUIRES_ONE_PATH_MSG)
            return 1
        prefix = self._normalize_prefix(s3_path)
        client = self._client()
        markers = SyncMarkers(sync_status_dir)
        failures = 0
        for rel_path in (manifest_rel, archive_rel):
            key = prefix + rel_path
            local_path = os.path.join(data_dir, rel_path)
            logger.info(f"download: s3://{self.bucket}/{key} to {local_path}")
            if dryrun:
                continue
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            try:
                client.download_file(self.bucket, key, local_path)
                if markers.enabled:
                    markers.write(rel_path, self._object_etag(client, key))
            except (ClientError, BotoCoreError, OSError) as e:
                failures += 1
                logger.info(f"\n*** Error ***\n{e}\n")
        if not dryrun and failures == 0:
            manifest_path = os.path.join(data_dir, manifest_rel)
            archive_root = self._archive_root_from_manifest(manifest_path)
            extract_failures = self._extract_archive(os.path.join(data_dir, archive_rel), data_dir, manifest_path)
            failures += extract_failures
            if extract_failures == 0:
                register_archive_path(sync_status_dir, archive_root or _normalize_filter_path(data_dir, paths[0]))
        elif dryrun:
            logger.info(f"extract archive: {os.path.join(data_dir, archive_rel)} to {data_dir}")
        return failures

    def _expand_local_archives(self, data_dir, dryrun=False, sync_status_dir=None):
        failures = 0
        for root, _, filenames in os.walk(data_dir):
            for filename in filenames:
                if not filename.endswith(ARCHIVE_EXT):
                    continue
                archive_path = os.path.join(root, filename)
                manifest_path = archive_path[:-len(ARCHIVE_EXT)] + ARCHIVE_MANIFEST_EXT
                if not os.path.exists(manifest_path):
                    continue
                manifest = self._read_archive_manifest(manifest_path)
                if not self._is_datakit_archive_manifest(manifest):
                    continue
                if dryrun:
                    logger.info(f"extract archive: {archive_path} to {data_dir}")
                    continue
                archive_root = manifest.get('root_path')
                extract_failures = self._extract_archive(archive_path, data_dir, manifest_path)
                failures += extract_failures
                if extract_failures == 0 and archive_root:
                    register_archive_path(sync_status_dir, archive_root)
        return failures

    def _read_archive_manifest(self, manifest_path):
        try:
            with open(manifest_path) as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None

    def _archive_root_from_manifest(self, manifest_path):
        manifest = self._read_archive_manifest(manifest_path)
        if manifest is None:
            return None
        return manifest.get('root_path')

    def _is_datakit_archive_manifest(self, manifest):
        return (
            isinstance(manifest, dict) and
            manifest.get('version') == 1 and
            manifest.get('archive_type') == 'zip' and
            isinstance(manifest.get('archive_path'), str) and
            isinstance(manifest.get('archive_sha256'), str) and
            isinstance(manifest.get('root_path'), str) and
            isinstance(manifest.get('files'), list)
        )

    def _extract_archive(self, archive_path, data_dir, manifest_path=None):
        logger.info(f"extract archive: {archive_path} to {data_dir}")
        try:
            if manifest_path:
                self._validate_archive_against_manifest(archive_path, data_dir, manifest_path)
            with zipfile.ZipFile(archive_path) as archive:
                data_root = os.path.abspath(data_dir)
                for member in archive.namelist():
                    target = os.path.abspath(os.path.join(data_dir, member))
                    if target != data_root and not target.startswith(data_root + os.sep):
                        raise OSError(f"Archive member escapes data dir: {member}")
                    if os.path.exists(target):
                        raise OSError(f"Archive extraction would overwrite existing path: {member}")
                archive.extractall(data_dir)
        except (OSError, zipfile.BadZipFile) as e:
            logger.info(f"\n*** Error ***\n{e}\n")
            return 1
        return 0

    def _validate_archive_against_manifest(self, archive_path, data_dir, manifest_path):
        manifest = self._read_archive_manifest(manifest_path)
        if not self._is_datakit_archive_manifest(manifest):
            raise OSError(f"Could not read archive manifest: {manifest_path}")
        expected_archive = _normalize_archive_manifest_path(manifest['archive_path'])
        if expected_archive is None:
            raise OSError(f"Archive manifest has an invalid archive path: {manifest_path}")
        archive_rel = _normalize_rel_path(os.path.relpath(archive_path, data_dir))
        if expected_archive != archive_rel:
            raise OSError(f"Archive path does not match manifest: {archive_rel} != {expected_archive}")
        root_path = _normalize_archive_manifest_path(manifest['root_path'])
        if root_path is None:
            raise OSError(f"Archive manifest has an invalid root path: {manifest_path}")
        if expected_archive != root_path + ARCHIVE_EXT:
            raise OSError(f"Archive root does not match manifest archive path: {root_path} != {expected_archive}")
        manifest_rel = _normalize_rel_path(os.path.relpath(manifest_path, data_dir))
        if manifest_rel != root_path + ARCHIVE_MANIFEST_EXT:
            raise OSError(f"Manifest path does not match archive root: {manifest_rel} != {root_path}")
        if _sha256_file(archive_path) != manifest['archive_sha256']:
            raise OSError(f"Archive checksum does not match manifest: {archive_path}")
        manifest_files = []
        manifest_sizes = {}
        for item in manifest['files']:
            if not isinstance(item, dict) or not isinstance(item.get('path'), str):
                raise OSError(f"Archive manifest has an invalid file entry: {manifest_path}")
            file_path = _normalize_archive_manifest_path(item['path'])
            if file_path is None:
                raise OSError(f"Archive manifest has an invalid file path: {manifest_path}")
            if not _is_under_archive_path(file_path, root_path):
                raise OSError(f"Archive manifest file is outside archive root: {file_path}")
            manifest_files.append(file_path)
            if 'size' in item:
                manifest_sizes[file_path] = item['size']
        with zipfile.ZipFile(archive_path) as archive:
            zip_infos = [info for info in archive.infolist() if not info.is_dir()]
        zip_paths = [_normalize_archive_manifest_path(info.filename) for info in zip_infos]
        if any(path is None for path in zip_paths):
            raise OSError(f"Archive member has an invalid path: {archive_path}")
        if sorted(zip_paths) != sorted(manifest_files):
            raise OSError(f"Archive members do not match manifest: {archive_path}")
        for info in zip_infos:
            rel_path = _normalize_archive_manifest_path(info.filename)
            expected_size = manifest_sizes.get(rel_path)
            if expected_size is not None and info.file_size != expected_size:
                raise OSError(f"Archive member size does not match manifest: {rel_path}")

    def compare(self, data_dir, s3_path='', sync_status_dir=None):
        """Compare local data files against the bucket's live listing.

        Returns a SyncComparison bucketing each rel_path by what changed since the sync
        recorded in its .synced marker: present on only one side, changed locally, changed
        on S3, changed on both (conflict), or present on both sides with no usable sync
        record to attribute the difference (differ). In-sync files are not reported.
        """
        markers = SyncMarkers(sync_status_dir)
        client = self._client()
        prefix = self._normalize_prefix(s3_path)
        local_files = list_data_files(data_dir, sync_status_dir=sync_status_dir, skip_archive_managed=True)
        remote_objects = self._list_s3_objects(client, prefix)
        changed_local, changed_s3, conflict, differ = [], [], [], []
        for rel_path in sorted(set(local_files) & set(remote_objects)):
            marker_etag, marker_mtime = markers.read(rel_path)
            if marker_etag is None:
                differ.append(rel_path)
                continue
            s3_changed = remote_objects[rel_path].etag != marker_etag
            local_changed = os.path.getmtime(local_files[rel_path]) > marker_mtime
            if local_changed and s3_changed:
                conflict.append(rel_path)
            elif local_changed:
                changed_local.append(rel_path)
            elif s3_changed:
                changed_s3.append(rel_path)
            # else: neither side changed since the last sync -> in sync, nothing to report
        return SyncComparison(
            only_local=sorted(set(local_files) - set(remote_objects)),
            only_s3=sorted(set(remote_objects) - set(local_files)),
            changed_local=changed_local,
            changed_s3=changed_s3,
            conflict=conflict,
            differ=differ,
        )

    def _client(self):
        session = boto3.Session(profile_name=self.user_profile)
        return session.client('s3')

    @staticmethod
    def _normalize_etag(etag):
        # boto3 surfaces the ETag wrapped in literal double quotes; strip them so the value we
        # store and compare is the bare hash.
        return etag.strip('"') if etag else etag

    def _object_etag(self, client, key):
        response = client.head_object(Bucket=self.bucket, Key=key)
        return self._normalize_etag(response.get('ETag'))

    def _upload(self, client, local_path, key, need_etag, callback=None):
        # Upload a local file to `key` and return the object's S3 ETag (quotes stripped). Small
        # files go via put_object, whose response carries the ETag, so no extra head_object is
        # needed; larger files keep upload_file's managed multipart transfer and we read the ETag
        # back with head_object only when a sync marker needs it (need_etag), else return None.
        content_type, _ = mimetypes.guess_type(local_path)
        if content_type is None:
            content_type = 'application/octet-stream'
        if os.path.getsize(local_path) < self.MULTIPART_THRESHOLD:
            with open(local_path, 'rb') as body:
                response = client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)
            if callback:
                callback(os.path.getsize(local_path))
            return self._normalize_etag(response.get('ETag'))
        extra_args = {'ContentType': content_type}
        if callback:
            client.upload_file(local_path, self.bucket, key, ExtraArgs=extra_args, Callback=callback)
        else:
            client.upload_file(local_path, self.bucket, key, ExtraArgs=extra_args)
        return self._object_etag(client, key) if need_etag else None

    def _normalize_prefix(self, s3_path):
        # Returns '' for falsy input so callers can concatenate key segments without a leading slash.
        if not s3_path:
            return ''
        return s3_path.strip('/') + '/'

    def _log_push_progress(self, processed, total, uploaded, skipped, failures, started):
        if processed and processed % self.PUSH_PROGRESS_INTERVAL == 0:
            elapsed = max(time.monotonic() - started, 0.001)
            rate = processed / elapsed
            logger.info(
                f"push progress: processed={processed}/{total} uploaded={uploaded} "
                f"skipped={skipped} failed={failures} rate={rate:.1f} files/s"
            )

    def _log_preflight_progress(self, processed, total, issues):
        if processed and processed % self.PUSH_PROGRESS_INTERVAL == 0:
            logger.info(f"push preflight: checked={processed}/{total} issue(s)={issues}")

    def _log_archive_preflight_progress(self, processed, total, issues):
        if processed and processed % self.PUSH_PROGRESS_INTERVAL == 0:
            logger.info(f"archive preflight: checked={processed}/{total} issue(s)={issues}")

    def _log_archive_build_progress(self, processed, total, archive_path, started):
        if processed and processed % self.PUSH_PROGRESS_INTERVAL == 0:
            elapsed = max(time.monotonic() - started, 0.001)
            rate = processed / elapsed
            size = self._format_bytes(os.path.getsize(archive_path))
            logger.info(
                f"archive build: archived={processed}/{total} size={size} "
                f"rate={rate:.1f} files/s"
            )

    def _log_archive_build_complete(self, archive_path, started):
        elapsed = max(time.monotonic() - started, 0.001)
        size = self._format_bytes(os.path.getsize(archive_path))
        logger.info(f"archive build: complete size={size} elapsed={elapsed:.1f}s")

    def _archive_upload_progress_callback(self, local_path, key):
        return _ArchiveUploadProgress(
            key,
            os.path.getsize(local_path),
            self.ARCHIVE_UPLOAD_PROGRESS_SECONDS,
            self.ARCHIVE_UPLOAD_PROGRESS_BYTES,
        )

    @staticmethod
    def _format_bytes(num_bytes):
        size = float(num_bytes)
        for unit in ('B', 'KB', 'MB', 'GB'):
            if size < 1024 or unit == 'GB':
                return f"{size:.1f} {unit}"
            size /= 1024

    def _log_push_summary(self, total, uploaded, skipped, failures, started):
        elapsed = max(time.monotonic() - started, 0.001)
        rate = total / elapsed
        logger.info(
            f"push summary: selected={total} uploaded={uploaded} skipped={skipped} "
            f"failed={failures} elapsed={elapsed:.1f}s rate={rate:.1f} files/s"
        )

    def _delete_keys(self, client, keys, progress_label=None):
        # delete_objects removes up to 1000 keys per request; batch accordingly.
        failures = 0
        total = len(keys)
        processed = 0
        started = time.monotonic()
        for start in range(0, len(keys), 1000):
            batch = keys[start:start + 1000]
            try:
                response = client.delete_objects(
                    Bucket=self.bucket,
                    Delete={'Objects': [{'Key': key} for key in batch]},
                )
            except (ClientError, BotoCoreError) as e:
                failures += len(batch)
                logger.info(f"\n*** Error ***\n{e}\n")
                processed += len(batch)
                self._log_delete_progress(progress_label, processed, total, failures, started)
                continue
            for error in response.get('Errors', []):
                failures += 1
                logger.info(f"\n*** Error ***\n{error.get('Key')}: {error.get('Message')}\n")
            processed += len(batch)
            self._log_delete_progress(progress_label, processed, total, failures, started)
        return failures

    def _log_delete_progress(self, progress_label, processed, total, failures, started):
        if not progress_label:
            return
        elapsed = max(time.monotonic() - started, 0.001)
        rate = processed / elapsed
        logger.info(
            f"{progress_label}: deleted={processed}/{total} failed={failures} "
            f"rate={rate:.1f} objects/s elapsed={elapsed:.1f}s"
        )

    def _is_directory_marker(self, key, prefix):
        rel_path = key[len(prefix):]
        return not rel_path or rel_path.endswith('/')

    def _list_s3_keys(self, client, prefix):
        keys = []
        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if not self._is_directory_marker(obj['Key'], prefix):
                    keys.append(obj['Key'])
        return keys

    def _list_s3_objects(self, client, prefix):
        objects = {}
        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                rel_path = obj['Key'][len(prefix):]
                if not self._is_directory_marker(obj['Key'], prefix):
                    objects[rel_path] = S3ObjectInfo(etag=self._normalize_etag(obj.get('ETag')))
        return objects
