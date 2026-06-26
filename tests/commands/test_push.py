import os
from unittest import mock

import pytest

from conftest import create_project_config
from datakit_data import Push


@pytest.fixture(autouse=True)
def initialize_data_configs(dkit_home, fake_project):
    project_configs = {
        's3_bucket': 'foo.org',
        's3_path': '2017/fake-project',
        'aws_user_profile': 'ap'
    }
    create_project_config(fake_project, project_configs)


def test_s3_instantiation(mocker):
    """
    S3 wrapper instantiated properly
    """
    s3_mock = mocker.patch(
        'datakit_data.commands.push.S3',
        autospec=True,
    )
    s3_mock.return_value.push.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = False
    cmd.run(parsed_args)
    # S3 instantiated with project-level configs for
    # user profile and bucket
    s3_mock.assert_called_once_with('ap', 'foo.org')


def test_push_invocation(mocker):
    """
    S3.push invoked with default data dir and s3 path
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = False
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir=None
    )


def test_boolean_cli_flags(mocker):
    """
    Remainder CLI args are prefixed with '--' and forwarded to S3.push.
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    parsed_args = mock.Mock()
    parsed_args.args = ['dryrun']
    parsed_args.sync_status_in_data = False
    cmd = Push(mock.Mock(), None, 'data push')
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=['--dryrun'],
        sync_status_dir=None
    )


def test_force_option_forwarded(mocker):
    """
    --force is parsed as an option and forwarded to S3.push.
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = cmd.get_parser('data push').parse_args(['--force'])
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=['--force'],
        sync_status_dir=None
    )


def test_verbose_option_forwarded(mocker):
    """
    --verbose is parsed as an option and forwarded to S3.push.
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = cmd.get_parser('data push').parse_args(['--verbose'])
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=['--verbose'],
        sync_status_dir=None
    )


def test_get_parser():
    """
    Push parser exposes 'args' and 'sync_status_in_data' attributes.
    """
    cmd = Push(mock.Mock(), None, 'data push')
    parser = cmd.get_parser('data push')
    args = parser.parse_args([])
    assert hasattr(args, 'args')
    assert hasattr(args, 'force')
    assert hasattr(args, 'verbose')
    assert hasattr(args, 'sync_status_in_data')
    assert hasattr(args, 'archive')
    assert hasattr(args, 'prune_individuals')
    assert hasattr(args, 'path')
    assert hasattr(args, 'include')
    assert hasattr(args, 'exclude')
    assert hasattr(args, 'jobs')


def test_filter_options_forwarded(mocker):
    """
    Targeting options are parsed as options and forwarded to S3.push.
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = cmd.get_parser('data push').parse_args([
        '--path', 'data/source/current',
        '--path', 'derived',
        '--include', '*.csv',
        '--exclude', 'tmp/*',
    ])
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir=None,
        paths=['data/source/current', 'derived'],
        include_patterns=['*.csv'],
        exclude_patterns=['tmp/*'],
    )


def test_archive_option_forwarded(mocker):
    """
    --archive is parsed and forwarded to S3.push with the selected path.
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = cmd.get_parser('data push').parse_args([
        '--archive',
        '--path', 'data/source/snapshot',
    ])
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir=None,
        paths=['data/source/snapshot'],
        archive=True,
    )


def test_archive_prune_individuals_option_forwarded(mocker):
    """
    --prune-individuals is parsed and forwarded to S3.push.
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = cmd.get_parser('data push').parse_args([
        '--archive',
        '--path', 'data/source/snapshot',
        '--prune-individuals',
    ])
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir=None,
        paths=['data/source/snapshot'],
        archive=True,
        prune_individuals=True,
    )


def test_jobs_option_forwarded(mocker):
    """
    --jobs is parsed and forwarded to S3.push when it changes the serial default.
    """
    push_mock = mocker.patch(
        'datakit_data.commands.push.S3.push',
        autospec=True,
    )
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = cmd.get_parser('data push').parse_args(['--jobs', '4'])
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir=None,
        jobs=4,
    )


def test_sync_status_in_data_writes_config(mocker, fake_project):
    """
    --sync-status-in-data persists 'data/' as sync_status_location in the project config,
    preserving all other keys.
    """
    create_project_config(fake_project, {
        's3_bucket': 'foo.org',
        's3_path': '2017/fake-project',
        'aws_user_profile': 'ap',
        'sync_status_location': '.sync_status',
    })
    push_mock = mocker.patch('datakit_data.commands.push.S3.push', autospec=True)
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = True
    cmd.run(parsed_args)
    from datakit.utils import read_json
    saved = read_json(os.path.join(fake_project, 'config', 'datakit-data.json'))
    assert saved['sync_status_location'] == 'data/'
    assert saved['s3_bucket'] == 'foo.org'
    assert saved['s3_path'] == '2017/fake-project'
    assert saved['aws_user_profile'] == 'ap'


def test_sync_status_in_data_flag_overrides_config(mocker, fake_project):
    """
    --sync-status-in-data forces sync_status_dir to 'data/' regardless of config
    """
    create_project_config(fake_project, {
        's3_bucket': 'foo.org',
        's3_path': '2017/fake-project',
        'aws_user_profile': 'ap',
        'sync_status_location': '.sync_status',
    })
    push_mock = mocker.patch('datakit_data.commands.push.S3.push', autospec=True)
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = True
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir='data/'
    )


def test_push_sync_status_alongside(mocker, fake_project):
    """
    sync_status_location set to 'data/' in config routes the sync status dir to 'data/'.
    """
    create_project_config(fake_project, {
        's3_bucket': 'foo.org',
        's3_path': '2017/fake-project',
        'aws_user_profile': 'ap',
        'sync_status_location': 'data/',
    })
    push_mock = mocker.patch('datakit_data.commands.push.S3.push', autospec=True)
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = False
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir='data/'
    )


def test_push_sync_status_separate_dir(mocker, fake_project):
    """
    sync_status_location set to a non-data dir passes that directory as the sync status dir.
    """
    create_project_config(fake_project, {
        's3_bucket': 'foo.org',
        's3_path': '2017/fake-project',
        'aws_user_profile': 'ap',
        'sync_status_location': '.sync_status',
    })
    push_mock = mocker.patch('datakit_data.commands.push.S3.push', autospec=True)
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = False
    cmd.run(parsed_args)
    push_mock.assert_any_call(
        mock.ANY,
        'data/',
        '2017/fake-project',
        extra_flags=[],
        sync_status_dir='.sync_status'
    )


def test_no_sync_status_location_creates_no_synced_files(mocker, fake_project):
    """
    When sync_status_location is absent from config, no .synced files are created after push.
    """
    data_dir = os.path.join(fake_project, 'data')
    os.makedirs(data_dir)
    open(os.path.join(data_dir, 'foo.csv'), 'w').close()
    mocker.patch('datakit_data.s3.boto3.Session')
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = False
    cmd.run(parsed_args)
    synced_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(fake_project)
        for f in files if f.endswith('.synced')
    ]
    assert synced_files == []


def test_unsupported_flag_warns(caplog, mocker):
    """
    An unsupported extra flag is reported and ignored; the push still runs.
    """
    push_mock = mocker.patch('datakit_data.commands.push.S3.push', autospec=True)
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = ['bogus']
    parsed_args.sync_status_in_data = False
    cmd.run(parsed_args)
    assert 'Ignoring unsupported flag(s): bogus' in caplog.text
    push_mock.assert_called_once()


def test_sync_status_in_data_dryrun_skips_config_write(mocker, fake_project):
    """
    A dry run with --sync-status-in-data does not persist sync_status_location to the config.
    """
    create_project_config(fake_project, {
        's3_bucket': 'foo.org',
        's3_path': '2017/fake-project',
        'aws_user_profile': 'ap',
        'sync_status_location': '.sync_status',
    })
    push_mock = mocker.patch('datakit_data.commands.push.S3.push', autospec=True)
    push_mock.return_value = 0
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = ['dryrun']
    parsed_args.sync_status_in_data = True
    cmd.run(parsed_args)
    from datakit.utils import read_json
    saved = read_json(os.path.join(fake_project, 'config', 'datakit-data.json'))
    assert saved['sync_status_location'] == '.sync_status'


def test_push_failures_exit_nonzero(caplog, mocker):
    """
    When S3.push reports transfer failures, the command exits non-zero and logs a summary.
    """
    push_mock = mocker.patch('datakit_data.commands.push.S3.push', autospec=True)
    push_mock.return_value = 2
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    parsed_args.sync_status_in_data = False
    assert cmd.run(parsed_args) == 1
    assert '2 file(s) failed to transfer' in caplog.text


def test_no_config_file(caplog):
    """
    Push logs a helpful message when the project config file is missing.
    """
    os.remove('config/datakit-data.json')
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    cmd.run(parsed_args)
    assert 'have you run `datakit data init`' in caplog.text


def test_empty_bucket(caplog, fake_project):
    """
    Push logs a warning when no S3 bucket is configured.
    """
    create_project_config(fake_project, {
        'aws_user_profile': 'ap',
        's3_bucket': '',
        's3_path': '2017/fake-project',
    })
    cmd = Push(mock.Mock(), None, 'data push')
    parsed_args = mock.Mock()
    parsed_args.args = []
    cmd.run(parsed_args)
    assert 'No bucket specified in config - no data pushed' in caplog.text
