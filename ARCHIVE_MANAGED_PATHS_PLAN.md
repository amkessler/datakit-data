# Archive-Managed Paths Behavior

## Goal

Make archive mode safe for mixed projects where `data/` contains both:

- normal per-file data that should be pushed as individual S3 objects, and
- archive-managed subtrees that should be pushed as zip archives plus manifests.

Without archive tracking, a later plain `datakit data push` would walk an extracted archive subtree
and upload all of its individual files. That defeats the purpose of archive mode.

## Behavior

1. `datakit data push --archive --path PATH`
   - Creates and uploads `PATH.zip` plus `PATH.manifest.json`.
   - Records `PATH` as archive-managed in local archive metadata.

2. Plain `datakit data push`
   - Reads local archive metadata.
   - Automatically skips individual files under archive-managed paths.
   - Logs which archive-managed paths were skipped.
   - Continues pushing normal per-file data elsewhere in `data/`.

3. `datakit data push --archive --path PATH`
   - Still works for archive-managed paths.
   - Rebuilds and uploads that archive.

4. `datakit data push --archive --path PATH --prune-individuals`
   - Converts a previously per-file path to archive mode.
   - Uploads `PATH.zip` and `PATH.manifest.json` first.
   - Deletes existing individual S3 objects below `PATH` only after the archive upload succeeds.
   - Leaves pruning opt-in because it is destructive.

5. Plain `datakit data pull`
   - Downloads archive zip and manifest sidecars as ordinary files when the archive has not been
     expanded locally.
   - Skips archive zip and manifest sidecars when the matching archive root already exists locally.
   - Records inferred archive-managed paths when a valid remote Datakit archive manifest matches
     an expanded local archive root.

6. `datakit data pull --expand-archives`
   - Extracts downloaded local archive files that have matching Datakit manifests.
   - Validates archive path, root path, checksum, member list, member sizes, and extraction targets.
   - Records successfully expanded archive roots as archive-managed.

7. `datakit data pull --archive --path PATH`
   - Downloads `PATH.manifest.json` and `PATH.zip`.
   - Extracts the archive after validation.
   - Records the manifest root, or `PATH` when the manifest root is unavailable, as archive-managed.

8. `datakit data pull delete`
   - Preserves extracted files below archive-managed paths.
   - Removes stale local archive zip and manifest sidecars for expanded archives unless `--force`
     is used.

9. `datakit data status` and `datakit data status --all`
   - Exclude expanded archive-managed local files from per-file status checks.
   - Suppress matching remote archive sidecars when the archive is expanded locally.
   - Continue to report archive zip and manifest objects as ordinary S3 objects when the archive has
     not been expanded locally.

## Metadata Format

Store metadata as JSON:

```json
{
  "version": 1,
  "archive_paths": [
    "source/current_snapshot"
  ]
}
```

Use the configured `sync_status_location` when present:

```text
<sync_status_location>/datakit-data-archives.json
```

When no sync status location is configured, use:

```text
.sync_status/datakit-data-archives.json
```

## Validation

- Archive push records the selected path.
- Regular push excludes files below recorded archive-managed paths.
- Regular push still uploads files outside those paths.
- Archive push still includes the archive-managed subtree contents.
- Archive prune only deletes individual S3 objects after archive upload succeeds.
- Archive prune preserves `PATH.zip` and `PATH.manifest.json`.
- Pull skips archive sidecars for expanded archives.
- Pull delete preserves expanded archive contents and removes stale local sidecars.
- Status and compare exclude expanded archive-managed local files and matching remote sidecars.
- Tests cover metadata read/write, skip behavior, extraction validation, delete preservation, and
  status/compare behavior.
