import os
from clusterjob.utils import run_cmd, _wrap_run_cmd

def test_mkdir(tmpdir):
    """Test that 'mkdir -p folder' actually creates folder"""
    folder = str(tmpdir.join('folder'))
    assert not os.path.isdir(folder)
    run_cmd(['mkdir', '-p', folder], remote=None, ignore_exit_code=False)
    assert os.path.isdir(folder)
    os.rmdir(folder)
    assert not os.path.isdir(folder)


def test_mkdir_wrapped(tmpdir):
    """Test that 'mkdir -p folder' actually creates folder, when wrapped by
    _wrap_run_cmd in record mode"""
    folder = str(tmpdir.join('folder'))
    jsonfile = str(tmpdir.join('run_cmd.json'))
    assert not os.path.isdir(folder)
    run_cmd = _wrap_run_cmd(jsonfile, mode='record')
    run_cmd(['mkdir', '-p', folder], remote=None, ignore_exit_code=False)
    assert os.path.isdir(folder)
    assert os.path.isfile(jsonfile)
