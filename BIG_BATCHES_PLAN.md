# Big Batch Syncing Plan

## Goal

Make `datakit data push` practical for projects with hundreds of thousands of small files by reducing unnecessary traversal, reducing log volume, failing early on bad local paths, allowing bounded parallel uploads, and preserving S3 listing correctness around directory-marker objects.

The work intentionally keeps the existing storage model: local files under `data/` map to S3 keys under the configured `s3_path`, and `.synced` markers remain the source of upload freshness. Archive mode and directory-level manifests are useful larger projects, but they change the storage contract enough that they should be planned separately after the immediate operational bottlenecks are addressed.

## Implementation Sequence

### 1. Targeted push filters

Add `datakit data push` options for selecting a subset of `data/`:

- `--path PATH`, repeatable, to restrict traversal to one or more files or subdirectories.
- `--include GLOB`, repeatable, to include only relative keys matching at least one glob.
- `--exclude GLOB`, repeatable, to omit relative keys matching any glob.

Paths should be accepted either relative to `data/` (`source/foo`) or with the leading `data/` prefix (`data/source/foo`). Filtering should happen during local file discovery so unchanged historical snapshots are not walked when a targeted path is supplied.

Validation:

- Add CLI tests showing the new options are parsed and forwarded to `S3.push`.
- Add S3 tests showing `--path` limits traversal and `--include`/`--exclude` select expected files.
- Run the focused push/S3 tests before moving on.

### 2. Quieter default logging with counters

Change push logging so per-file `skipped:` lines are opt-in. Default output should show summary/progress information instead of one line per unchanged file.

Initial behavior:

- Keep per-file `upload:` lines by default for files actually transferred.
- Suppress per-file `skipped:` lines unless `--verbose` is set.
- Log a final summary with discovered, selected, uploaded, skipped, failed, and elapsed time.
- Log periodic progress summaries for large pushes.

Validation:

- Update existing skip logging tests for quiet default behavior.
- Add tests that `--verbose` restores per-file skipped output.
- Add tests for summary counters.
- Run the focused push/S3 tests before moving on.

### 3. Preflight validation

Validate candidate files before upload starts so broken symlinks and unreadable/non-regular paths are reported together instead of failing late after many skips/uploads.

Initial validation should catch:

- Broken symlinks.
- Non-regular files.
- Files that cannot be statted or opened for reading.

Preflight should run after filtering, so targeted pushes do not fail because of unrelated bad files elsewhere in `data/`. If preflight finds errors, push should log each error, log a count, return a nonzero failure count, and perform no uploads.

Validation:

- Add tests for broken symlink failure before upload.
- Add tests that a targeted push does not inspect unrelated broken symlinks outside the selected path.
- Run the focused push/S3 tests before moving on.

### 4. Configurable parallel S3 uploads

Add bounded thread-pool uploads for files that actually need transfer.

Initial behavior:

- Default remains serial (`--jobs 1`) to preserve existing behavior and reduce surprise.
- `--jobs N` enables concurrent uploads with one boto3 client per worker.
- Failure counts and marker writes must remain correct.
- Logging should remain readable; progress summaries should be emitted from the main thread as futures complete.

Validation:

- Add CLI tests showing `--jobs` is parsed and forwarded.
- Add S3 tests showing multiple worker clients are created when `jobs > 1`.
- Add S3 tests showing upload failures are aggregated correctly in parallel mode.
- Run the focused push/S3 tests before moving on.

### 5. Ignore S3 directory markers

Restore filtering for S3 console directory-marker objects in `_list_s3_keys()` and `_list_s3_objects()`.

Behavior:

- Ignore an object equal to the listing prefix.
- Ignore objects whose relative path ends with `/`.
- Keep real object keys under those prefixes.

Validation:

- Add tests for `_list_s3_keys()` and `_list_s3_objects()` with root and nested directory markers.
- Run the focused push/S3 tests before moving on.

## Final Verification

After all steps:

- Run the full test suite with `uv run pytest`.
- Review the full diff for behavioral regressions, edge cases, and missing tests.
- Patch any review findings and rerun the affected tests plus the full suite.

## Out Of Scope For This Branch

- Directory-level manifests.
- Archive mode for immutable snapshots.
- Checkpoint/resume by traversal position.
- Parallel pull/download behavior.
