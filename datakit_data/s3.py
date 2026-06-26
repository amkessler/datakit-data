import os
import logging
import mimetypes
import fnmatch
import time
import threading
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


def list_local_files(data_dir, paths=None, include_patterns=None, exclude_patterns=None):
    # Map of rel_path -> full path for every data file under data_dir, excluding .synced
    # markers (which live alongside the data when sync_status_location is data/). The key is
    # used to build/compare S3 keys, which always use '/'; normalize the OS separator so keys
    # generated on Windows match remote keys, while the value stays OS-native for filesystem
    # operations.
    paths = paths or []
    include_patterns = [_strip_data_prefix(pattern) for pattern in include_patterns or []]
    exclude_patterns = [_strip_data_prefix(pattern) for pattern in exclude_patterns or []]
    files = {}
    for root_path in _candidate_roots(data_dir, paths):
        if os.path.isfile(root_path) or os.path.islink(root_path):
            candidates = [(os.path.dirname(root_path), [os.path.basename(root_path)])]
        elif os.path.isdir(root_path):
            candidates = ((root, filenames) for root, _, filenames in os.walk(root_path))
        else:
            continue
        for root, filenames in candidates:
            for filename in filenames:
                if filename.endswith(SyncMarkers.SUFFIX):
                    continue
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, data_dir).replace(os.sep, '/')
                if not _selected_by_patterns(rel_path, include_patterns, exclude_patterns):
                    continue
                files[rel_path] = full_path
    return files


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

    def __init__(self, aws_user_profile, s3_bucket):
        self.user_profile = aws_user_profile
        self.bucket = s3_bucket

    def push(
        self, data_dir, s3_path='', extra_flags=None, sync_status_dir=None,
        paths=None, include_patterns=None, exclude_patterns=None, jobs=1
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
        markers = SyncMarkers(sync_status_dir)
        failures = 0
        logger.info("push discovery: scanning local files")
        local_files = list_local_files(
            data_dir,
            paths=paths,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
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
            to_delete = [prefix + rel_path for rel_path in sorted(remote_rel - set(local_files.keys()))]
            for key in to_delete:
                logger.info(f"delete: s3://{self.bucket}/{key}")
            if not dryrun:
                failures += self._delete_keys(client, to_delete)
        self._log_push_summary(total, uploaded, skipped, failures, started)
        return failures

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

    def pull(self, data_dir, s3_path='', extra_flags=None, sync_status_dir=None):
        extra_flags = extra_flags or []
        dryrun = '--dryrun' in extra_flags or '--dry-run' in extra_flags
        delete = '--delete' in extra_flags
        force = '--force' in extra_flags
        prefix = self._normalize_prefix(s3_path)
        if delete and not prefix:
            logger.info(EMPTY_PATH_DELETE_MSG)
            return 1
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
            local_files = list_local_files(data_dir)
            remote_rel = set(remote_objects)
            for rel_path, local_path in sorted(local_files.items()):
                if rel_path not in remote_rel:
                    logger.info(f"delete: {local_path}")
                    if not dryrun:
                        try:
                            os.remove(local_path)
                        except OSError as e:
                            failures += 1
                            logger.info(f"\n*** Error ***\n{e}\n")
        return failures

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
        local_files = list_local_files(data_dir)
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

    def _upload(self, client, local_path, key, need_etag):
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
            return self._normalize_etag(response.get('ETag'))
        client.upload_file(local_path, self.bucket, key, ExtraArgs={'ContentType': content_type})
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

    def _log_push_summary(self, total, uploaded, skipped, failures, started):
        elapsed = max(time.monotonic() - started, 0.001)
        rate = total / elapsed
        logger.info(
            f"push summary: selected={total} uploaded={uploaded} skipped={skipped} "
            f"failed={failures} elapsed={elapsed:.1f}s rate={rate:.1f} files/s"
        )

    def _delete_keys(self, client, keys):
        # delete_objects removes up to 1000 keys per request; batch accordingly.
        failures = 0
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
                continue
            for error in response.get('Errors', []):
                failures += 1
                logger.info(f"\n*** Error ***\n{error.get('Key')}: {error.get('Message')}\n")
        return failures

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
