import os
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError, EndpointConnectionError

from datakit_data.s3 import S3


def _remote_object(size=0, last_modified=None):
    if last_modified is None:
        last_modified = datetime.now(tz=timezone.utc)
    return {
        'Size': size,
        'LastModified': last_modified,
        'ETag': '"fake-etag"',
    }


def _make_file(path, content='', mtime=None):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)
    if mtime is not None:
        os.utime(path, (mtime, mtime))


def test_push(mocker):
    """
    S3.push uploads each local file that is missing from S3 to the correct S3 key.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={
        'foo': 'data/foo', 'bar': 'data/bar'
    })
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mocker.patch.object(S3, '_set_local_mtime')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '2017/fake-project')

    assert result == 0
    mock_session.assert_called_once_with(profile_name='ap')
    upload_calls = {call[0] for call in mock_client.upload_file.call_args_list}
    assert ('data/foo', 'foo.org', '2017/fake-project/foo') in upload_calls
    assert ('data/bar', 'foo.org', '2017/fake-project/bar') in upload_calls


def test_pull(mocker):
    """
    S3.pull downloads each S3 object that is missing locally to the correct local path.
    """
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo': _remote_object(),
        'bar': _remote_object(),
    })
    mocker.patch.object(S3, '_set_local_mtime')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mocker.patch('datakit_data.s3.os.makedirs')

    s3 = S3('ap', 'foo.org')
    result = s3.pull('data/', '2017/fake-project')

    assert result == 0
    mock_session.assert_called_once_with(profile_name='ap')
    download_calls = {call[0] for call in mock_client.download_file.call_args_list}
    assert ('foo.org', '2017/fake-project/foo', 'data/foo') in download_calls
    assert ('foo.org', '2017/fake-project/bar', 'data/bar') in download_calls


def test_push_creates_sync_markers(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    sync_dir = str(tmpdir.mkdir('sync'))
    open(os.path.join(data_dir, 'foo.csv'), 'w').close()
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.head_object.return_value = {'LastModified': datetime.now(tz=timezone.utc)}

    s3 = S3('ap', 'foo.org')
    s3.push(data_dir, '2017/fake-project', sync_status_dir=sync_dir)

    assert os.path.exists(os.path.join(sync_dir, 'foo.csv.synced'))


def test_push_skips_synced_files(mocker):
    """
    S3.push does not upload .synced marker files to S3.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={
        'foo': 'data/foo',
        'foo.synced': 'data/foo.synced',
        'subdir/bar.synced': 'data/subdir/bar.synced',
    })
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mocker.patch.object(S3, '_set_local_mtime')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    s3.push('data/', '2017/fake-project')

    upload_calls = {call[0] for call in mock_client.upload_file.call_args_list}
    assert ('data/foo', 'foo.org', '2017/fake-project/foo') in upload_calls
    assert not any('.synced' in call[2] for call in upload_calls)


def test_push_dryrun(mocker):
    """
    S3.push with --dryrun logs intended uploads without calling upload_file.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={'foo': 'data/foo'})
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    s3.push('data/', '2017/fake-project', extra_flags=['--dryrun'])

    mock_client.upload_file.assert_not_called()


def test_push_skips_current_remote_file_and_refreshes_marker(mocker, tmpdir):
    """
    S3.push skips upload when size matches and S3 is as new as the local file,
    but still creates a sync marker because the remote object is confirmed current.
    """
    data_dir = str(tmpdir.mkdir('data'))
    sync_dir = str(tmpdir.mkdir('sync'))
    local_path = os.path.join(data_dir, 'foo.csv')
    local_mtime = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    _make_file(local_path, content='abc', mtime=local_mtime)
    remote_mtime = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=3, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.push(data_dir, '2017/fake-project', sync_status_dir=sync_dir)

    assert result == 0
    mock_client.upload_file.assert_not_called()
    assert os.path.exists(os.path.join(sync_dir, 'foo.csv.synced'))


def test_push_uploads_when_local_file_is_newer(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    local_path = os.path.join(data_dir, 'foo.csv')
    _make_file(local_path, content='abc')
    remote_mtime = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=3, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.head_object.return_value = {'LastModified': datetime.now(tz=timezone.utc)}

    s3 = S3('ap', 'foo.org')
    result = s3.push(data_dir, '2017/fake-project')

    assert result == 0
    mock_client.upload_file.assert_called_once_with(
        local_path,
        'foo.org',
        '2017/fake-project/foo.csv',
    )


def test_push_uploads_when_size_differs(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    local_path = os.path.join(data_dir, 'foo.csv')
    _make_file(local_path, content='abc')
    remote_mtime = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=4, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.head_object.return_value = {'LastModified': datetime.now(tz=timezone.utc)}

    s3 = S3('ap', 'foo.org')
    result = s3.push(data_dir, '2017/fake-project')

    assert result == 0
    mock_client.upload_file.assert_called_once_with(
        local_path,
        'foo.org',
        '2017/fake-project/foo.csv',
    )


def test_push_dryrun_does_not_create_sync_marker(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    sync_dir = str(tmpdir.mkdir('sync'))
    local_path = os.path.join(data_dir, 'foo.csv')
    _make_file(local_path, content='abc')
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.push(data_dir, '2017/fake-project', extra_flags=['--dryrun'], sync_status_dir=sync_dir)

    assert result == 0
    mock_client.upload_file.assert_not_called()
    assert not os.path.exists(os.path.join(sync_dir, 'foo.csv.synced'))


def test_pull_dryrun(mocker):
    """
    S3.pull with --dryrun logs intended downloads without calling download_file.
    """
    mocker.patch.object(S3, '_list_s3_objects', return_value={'foo': _remote_object()})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    s3.pull('data/', '2017/fake-project', extra_flags=['--dryrun'])

    mock_client.download_file.assert_not_called()


def test_pull_skips_current_local_file(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    local_path = os.path.join(data_dir, 'foo.csv')
    local_mtime = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc).timestamp()
    _make_file(local_path, content='abc', mtime=local_mtime)
    remote_mtime = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=3, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.pull(data_dir, '2017/fake-project')

    assert result == 0
    mock_client.download_file.assert_not_called()


def test_pull_downloads_when_remote_object_is_newer(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    local_path = os.path.join(data_dir, 'foo.csv')
    local_mtime = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    _make_file(local_path, content='abc', mtime=local_mtime)
    remote_mtime = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=3, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.pull(data_dir, '2017/fake-project')

    assert result == 0
    mock_client.download_file.assert_called_once_with(
        'foo.org',
        '2017/fake-project/foo.csv',
        local_path,
    )


def test_push_sets_local_mtime_to_uploaded_object_last_modified(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    sync_dir = str(tmpdir.mkdir('sync'))
    local_path = os.path.join(data_dir, 'foo.csv')
    marker_path = os.path.join(sync_dir, 'foo.csv.synced')
    local_mtime = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    _make_file(local_path, content='abc', mtime=local_mtime)
    uploaded_mtime = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.head_object.return_value = {'LastModified': uploaded_mtime}

    s3 = S3('ap', 'foo.org')
    result = s3.push(data_dir, '2017/fake-project', sync_status_dir=sync_dir)

    assert result == 0
    mock_client.head_object.assert_called_once_with(
        Bucket='foo.org',
        Key='2017/fake-project/foo.csv',
    )
    assert abs(os.path.getmtime(local_path) - uploaded_mtime.timestamp()) < 0.001
    assert abs(os.path.getmtime(marker_path) - uploaded_mtime.timestamp()) < 0.001


def test_pull_sets_local_mtime_to_remote_object_last_modified(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    local_path = os.path.join(data_dir, 'foo.csv')
    local_mtime = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    _make_file(local_path, content='abc', mtime=local_mtime)
    remote_mtime = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=3, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.pull(data_dir, '2017/fake-project')

    assert result == 0
    mock_client.download_file.assert_called_once_with(
        'foo.org',
        '2017/fake-project/foo.csv',
        local_path,
    )
    assert abs(os.path.getmtime(local_path) - remote_mtime.timestamp()) < 0.001


def test_pull_downloads_when_size_differs(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    local_path = os.path.join(data_dir, 'foo.csv')
    _make_file(local_path, content='abc')
    remote_mtime = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=4, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.pull(data_dir, '2017/fake-project')

    assert result == 0
    mock_client.download_file.assert_called_once_with(
        'foo.org',
        '2017/fake-project/foo.csv',
        local_path,
    )


def test_pull_skips_when_local_file_is_newer(mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    local_path = os.path.join(data_dir, 'foo.csv')
    _make_file(local_path, content='abc')
    remote_mtime = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo.csv': _remote_object(size=3, last_modified=remote_mtime)
    })
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.pull(data_dir, '2017/fake-project')

    assert result == 0
    mock_client.download_file.assert_not_called()


def test_pull_ignores_s3_directory_markers(mocker):
    """
    S3.pull ignores zero-byte S3 directory marker objects that end in '/'.
    """
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.pull('data/', '2017/fake-project')

    assert result == 0
    mock_client.download_file.assert_not_called()


def test_push_delete(mocker):
    """
    S3.push with --delete batch-removes S3 keys that have no corresponding local file.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={'foo': 'data/foo'})
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'stale': _remote_object(),
    })
    mocker.patch.object(S3, '_set_local_mtime')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.delete_objects.return_value = {'Deleted': [{'Key': '2017/fake-project/stale'}]}

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '2017/fake-project', extra_flags=['--delete'])

    assert result == 0
    mock_client.delete_object.assert_not_called()
    mock_client.delete_objects.assert_called_once_with(
        Bucket='foo.org',
        Delete={'Objects': [{'Key': '2017/fake-project/stale'}]},
    )


def test_pull_delete(mocker):
    """
    S3.pull with --delete removes local files that are absent from S3.
    """
    mocker.patch.object(S3, '_list_s3_objects', return_value={'foo': _remote_object()})
    mocker.patch.object(S3, '_should_download', return_value=(False, 'local file is current'))
    mocker.patch.object(S3, '_list_local_files', return_value={
        'foo': 'data/foo',
        'stale': 'data/stale',
    })
    mocker.patch('datakit_data.s3.boto3.Session')
    mocker.patch('datakit_data.s3.os.makedirs')
    mock_remove = mocker.patch('datakit_data.s3.os.remove')

    s3 = S3('ap', 'foo.org')
    s3.pull('data/', '2017/fake-project', extra_flags=['--delete'])

    mock_remove.assert_called_once_with('data/stale')


def test_pull_delete_ignores_synced_marker_files(mocker):
    """
    S3.pull with --delete ignores .synced marker files when deciding which local
    files are absent from S3.
    """
    mocker.patch.object(S3, '_list_s3_objects', return_value={'foo': _remote_object()})
    mocker.patch.object(S3, '_should_download', return_value=(False, 'local file is current'))
    mocker.patch.object(S3, '_list_local_files', return_value={
        'foo': 'data/foo',
        'foo.synced': 'data/foo.synced',
        'subdir/bar.synced': 'data/subdir/bar.synced',
        'stale': 'data/stale',
    })
    mocker.patch('datakit_data.s3.boto3.Session')
    mock_remove = mocker.patch('datakit_data.s3.os.remove')

    s3 = S3('ap', 'foo.org')
    s3.pull('data/', '2017/fake-project', extra_flags=['--delete'])

    mock_remove.assert_called_once_with('data/stale')


def test_push_client_error(caplog, mocker):
    """
    S3.push logs an error message and counts the failure when boto3 raises a ClientError.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={'foo': 'data/foo'})
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.upload_file.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}, 'PutObject'
    )

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '2017/fake-project')

    assert result == 1
    assert '*** Error ***' in caplog.text


def test_push_failed_upload_does_not_create_sync_marker(caplog, mocker, tmpdir):
    data_dir = str(tmpdir.mkdir('data'))
    sync_dir = str(tmpdir.mkdir('sync'))
    local_path = os.path.join(data_dir, 'foo.csv')
    _make_file(local_path, content='abc')
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.upload_file.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}, 'PutObject'
    )

    s3 = S3('ap', 'foo.org')
    result = s3.push(data_dir, '2017/fake-project', sync_status_dir=sync_dir)

    assert result == 1
    assert '*** Error ***' in caplog.text
    assert not os.path.exists(os.path.join(sync_dir, 'foo.csv.synced'))


def test_push_connection_error(caplog, mocker):
    """
    S3.push also catches non-ClientError botocore errors (e.g. connection failures).
    """
    mocker.patch.object(S3, '_list_local_files', return_value={'foo': 'data/foo'})
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.upload_file.side_effect = EndpointConnectionError(endpoint_url='https://s3')

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '2017/fake-project')

    assert result == 1
    assert '*** Error ***' in caplog.text


def test_push_delete_batch_error(caplog, mocker):
    """
    S3.push counts every key in a batch as failed when delete_objects raises.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={'foo': 'data/foo'})
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'stale1': _remote_object(),
        'stale2': _remote_object(),
    })
    mocker.patch.object(S3, '_set_local_mtime')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.delete_objects.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}, 'DeleteObjects'
    )

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '2017/fake-project', extra_flags=['--delete'])

    assert result == 2
    assert '*** Error ***' in caplog.text


def test_push_delete_partial_error(caplog, mocker):
    """
    S3.push counts per-key Errors reported in the delete_objects response.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={'foo': 'data/foo'})
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'stale1': _remote_object(),
        'stale2': _remote_object(),
    })
    mocker.patch.object(S3, '_set_local_mtime')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.delete_objects.return_value = {
        'Deleted': [{'Key': '2017/fake-project/stale1'}],
        'Errors': [{'Key': '2017/fake-project/stale2', 'Message': 'Access Denied'}],
    }

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '2017/fake-project', extra_flags=['--delete'])

    assert result == 1
    assert '2017/fake-project/stale2' in caplog.text


def test_push_delete_empty_path_refused(caplog, mocker):
    """
    S3.push refuses --delete when s3_path normalizes to an empty prefix (whole-bucket scope),
    aborting before any S3 client is created or files are listed.
    """
    list_local = mocker.patch.object(S3, '_list_local_files')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '', extra_flags=['--delete'])

    assert result == 1
    assert 'Refusing --delete' in caplog.text
    mock_session.assert_not_called()
    list_local.assert_not_called()


def test_push_empty_path_without_delete_allowed(mocker):
    """
    An empty s3_path is allowed without --delete (e.g. a dedicated bucket); keys are built
    without a leading slash.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={'foo': 'data/foo'})
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mocker.patch.object(S3, '_set_local_mtime')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value

    s3 = S3('ap', 'foo.org')
    result = s3.push('data/', '', extra_flags=[])

    assert result == 0
    mock_client.upload_file.assert_called_once_with('data/foo', 'foo.org', 'foo')


def test_pull_delete_empty_path_refused(caplog, mocker):
    """
    S3.pull refuses --delete when s3_path normalizes to an empty prefix (whole-bucket scope),
    aborting before any S3 client is created or keys are listed.
    """
    list_objects = mocker.patch.object(S3, '_list_s3_objects')
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')

    s3 = S3('ap', 'foo.org')
    result = s3.pull('data/', '', extra_flags=['--delete'])

    assert result == 1
    assert 'Refusing --delete' in caplog.text
    mock_session.assert_not_called()
    list_objects.assert_not_called()


def test_pull_client_error(caplog, mocker):
    """
    S3.pull logs an error message when boto3 raises a ClientError.
    """
    mocker.patch.object(S3, '_list_s3_objects', return_value={'foo': _remote_object()})
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_client.download_file.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}, 'GetObject'
    )
    mocker.patch('datakit_data.s3.os.makedirs')

    s3 = S3('ap', 'foo.org')
    result = s3.pull('data/', '2017/fake-project')

    assert result == 1
    assert '*** Error ***' in caplog.text


def test_push_logging(caplog, mocker):
    """
    S3.push logs an 'upload:' line for each file transferred.
    """
    mocker.patch.object(S3, '_list_local_files', return_value={
        'foo': 'data/foo', 'bar': 'data/bar'
    })
    mocker.patch.object(S3, '_list_s3_objects', return_value={})
    mocker.patch.object(S3, '_set_local_mtime')
    mocker.patch('datakit_data.s3.boto3.Session')

    s3 = S3('ap', 'foo.org')
    s3.push('data/', '2017/fake-project')

    assert 'upload: data/foo to s3://foo.org/2017/fake-project/foo' in caplog.text
    assert 'upload: data/bar to s3://foo.org/2017/fake-project/bar' in caplog.text


def test_pull_logging(caplog, mocker):
    """
    S3.pull logs a 'download:' line for each file transferred.
    """
    mocker.patch.object(S3, '_list_s3_objects', return_value={
        'foo': _remote_object(),
        'bar': _remote_object(),
    })
    mocker.patch.object(S3, '_set_local_mtime')
    mocker.patch('datakit_data.s3.boto3.Session')
    mocker.patch('datakit_data.s3.os.makedirs')

    s3 = S3('ap', 'foo.org')
    s3.pull('data/', '2017/fake-project')

    assert 'download: s3://foo.org/2017/fake-project/foo to data/foo' in caplog.text
    assert 'download: s3://foo.org/2017/fake-project/bar to data/bar' in caplog.text


def test_list_local_files(tmpdir):
    """
    _list_local_files returns a relative-key → absolute-path mapping for files in the given directory.
    """
    data_dir = str(tmpdir.mkdir('data'))
    open(os.path.join(data_dir, 'foo'), 'w').close()
    open(os.path.join(data_dir, 'bar'), 'w').close()

    s3 = S3('ap', 'foo.org')
    result = s3._list_local_files(data_dir)

    assert 'foo' in result
    assert 'bar' in result
    assert result['foo'] == os.path.join(data_dir, 'foo')


def test_list_local_files_nested_keys_use_forward_slashes(tmpdir):
    """
    Keys for files in subdirectories use forward slashes (matching S3 key syntax).
    """
    data_dir = str(tmpdir.mkdir('data'))
    nested = os.path.join(data_dir, 'sub')
    os.makedirs(nested)
    open(os.path.join(nested, 'foo.csv'), 'w').close()

    s3 = S3('ap', 'foo.org')
    result = s3._list_local_files(data_dir)

    assert 'sub/foo.csv' in result
    assert result['sub/foo.csv'] == os.path.join(nested, 'foo.csv')


def test_list_local_files_normalizes_windows_separator(mocker):
    """
    On Windows (os.sep == '\\') the relative key is normalized to forward slashes so the
    generated S3 keys match remote keys; the value stays OS-native.
    """
    mocker.patch('datakit_data.s3.os.path.isdir', return_value=True)
    mocker.patch('datakit_data.s3.os.walk', return_value=[('data\\sub', [], ['foo.csv'])])
    mocker.patch('datakit_data.s3.os.path.join', side_effect=lambda *parts: '\\'.join(parts))
    mocker.patch('datakit_data.s3.os.path.relpath', return_value='sub\\foo.csv')
    mocker.patch('datakit_data.s3.os.sep', '\\')

    s3 = S3('ap', 'foo.org')
    result = s3._list_local_files('data')

    assert 'sub/foo.csv' in result
    assert result['sub/foo.csv'] == 'data\\sub\\foo.csv'


def test_list_local_files_missing_dir():
    """
    _list_local_files returns an empty dict when the directory does not exist.
    """
    s3 = S3('ap', 'foo.org')
    result = s3._list_local_files('/nonexistent/path/data')
    assert result == {}


def test_list_s3_keys(mocker):
    """
    _list_s3_keys paginates the S3 listing and returns all matching keys.
    """
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_paginator = mock_client.get_paginator.return_value
    mock_paginator.paginate.return_value = [
        {'Contents': [{'Key': '2017/foo'}, {'Key': '2017/bar'}]}
    ]

    s3 = S3('ap', 'foo.org')
    client = s3._client()
    result = s3._list_s3_keys(client, '2017/')

    mock_client.get_paginator.assert_called_with('list_objects_v2')
    mock_paginator.paginate.assert_called_with(Bucket='foo.org', Prefix='2017/')
    assert result == ['2017/foo', '2017/bar']


def test_list_s3_keys_empty_page(mocker):
    """
    _list_s3_keys returns an empty list when the S3 response page has no Contents.
    """
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_paginator = mock_client.get_paginator.return_value
    mock_paginator.paginate.return_value = [{}]

    s3 = S3('ap', 'foo.org')
    client = s3._client()
    result = s3._list_s3_keys(client, '2017/')

    assert result == []


def test_list_s3_objects(mocker):
    from datetime import datetime, timezone
    last_modified = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_paginator = mock_client.get_paginator.return_value
    mock_paginator.paginate.return_value = [{'Contents': [
        {'Key': '2017/foo', 'Size': 10, 'LastModified': last_modified, 'ETag': '"foo"'},
        {'Key': '2017/bar', 'Size': 20, 'LastModified': last_modified, 'ETag': '"bar"'},
    ]}]

    s3 = S3('ap', 'foo.org')
    client = s3._client()
    result = s3._list_s3_objects(client, '2017/')

    mock_paginator.paginate.assert_called_with(Bucket='foo.org', Prefix='2017/')
    assert result == {
        'foo': {'Size': 10, 'LastModified': last_modified, 'ETag': '"foo"'},
        'bar': {'Size': 20, 'LastModified': last_modified, 'ETag': '"bar"'},
    }


def test_list_s3_objects_ignores_directory_markers(mocker):
    from datetime import datetime, timezone
    last_modified = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_paginator = mock_client.get_paginator.return_value
    mock_paginator.paginate.return_value = [{'Contents': [
        {'Key': '2017/source/', 'Size': 0, 'LastModified': last_modified, 'ETag': '"dir"'},
        {'Key': '2017/source/foo.csv', 'Size': 10, 'LastModified': last_modified, 'ETag': '"foo"'},
    ]}]

    s3 = S3('ap', 'foo.org')
    client = s3._client()
    result = s3._list_s3_objects(client, '2017/')

    assert result == {
        'source/foo.csv': {'Size': 10, 'LastModified': last_modified, 'ETag': '"foo"'},
    }


def test_list_s3_objects_empty_page(mocker):
    mock_session = mocker.patch('datakit_data.s3.boto3.Session')
    mock_client = mock_session.return_value.client.return_value
    mock_paginator = mock_client.get_paginator.return_value
    mock_paginator.paginate.return_value = [{}]

    s3 = S3('ap', 'foo.org')
    client = s3._client()
    result = s3._list_s3_objects(client, '2017/')

    assert result == {}


def test_normalize_prefix():
    """
    _normalize_prefix strips leading slashes and ensures a single trailing slash.
    """
    s3 = S3('ap', 'foo.org')
    assert s3._normalize_prefix('2017/fake-project') == '2017/fake-project/'
    assert s3._normalize_prefix('/2017/fake-project/') == '2017/fake-project/'
    assert s3._normalize_prefix('') == ''
