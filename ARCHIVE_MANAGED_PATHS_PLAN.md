# Archive-Managed Paths Plan

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
- Tests cover metadata read/write and skip behavior.
