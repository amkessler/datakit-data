# Run This Branch Locally For Testing

Use these steps to test the local `datakit-data` branch instead of the globally installed plugin.

## Option 1: Per-Repo Virtual Environment

Use this when you want each test repo to have its own isolated environment.

From the repo you want to test:

```bash
cd /path/to/test-project

uv venv --python 3.12
uv pip install -e /Users/akessler/GITREPOS/github_kessler/datakit-data
```

Verify that `uv run` is using this local branch:

```bash
uv run python -c "import datakit_data.s3, inspect; print(inspect.getfile(datakit_data.s3))"
```

Expected output:

```text
/Users/akessler/GITREPOS/github_kessler/datakit-data/datakit_data/s3.py
```

Run Datakit through `uv run`:

```bash
uv run datakit data push --help
uv run datakit data push --path data/source/current_snapshot dryrun
uv run datakit data push --path data/source/current_snapshot --jobs 4
```

Repeat this setup in each repo directory you want to test if you want repo-local isolation.

## Option 2: Shared Test Virtual Environment

Use this when you want to test several repos quickly with the same local branch install.

Create the shared environment once:

```bash
uv venv /tmp/datakit-data-test-venv --python 3.12
uv pip install --python /tmp/datakit-data-test-venv/bin/python -e /Users/akessler/GITREPOS/github_kessler/datakit-data
```

Verify the shared environment:

```bash
/tmp/datakit-data-test-venv/bin/python -c "import datakit_data.s3, inspect; print(inspect.getfile(datakit_data.s3))"
```

Expected output:

```text
/Users/akessler/GITREPOS/github_kessler/datakit-data/datakit_data/s3.py
```

From any repo you want to test:

```bash
cd /path/to/test-project

/tmp/datakit-data-test-venv/bin/datakit data push --help
/tmp/datakit-data-test-venv/bin/datakit data push --path data/source/current_snapshot dryrun
/tmp/datakit-data-test-venv/bin/datakit data push --path data/source/current_snapshot --jobs 4
```

## Notes

- Use `uv run datakit ...` only after installing the editable local branch in that repo's `.venv`.
- Use `/tmp/datakit-data-test-venv/bin/datakit ...` when using the shared test environment.
- Avoid plain `datakit ...` unless you intentionally want the globally installed version.
- Start with `dryrun` before any real push.
- Test serial behavior first, then try parallel uploads with a modest worker count such as `--jobs 4`.
- Filtered pushes cannot be combined with `delete`; run an unfiltered delete separately if needed.
