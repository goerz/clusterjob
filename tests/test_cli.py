from __future__ import print_function
import os
import clusterjob.cli
from clusterjob.cli import test_backend as cli_backend_tester
import click
from click.testing import CliRunner
import pytest
try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock
# builtin fixtures: monkeypatch, request


def test_cli_backend_tester(monkeypatch, request):
    test_module = request.module.__file__
    test_dir, _ = os.path.splitext(test_module)
    runner = CliRunner()

    result = runner.invoke(cli_backend_tester, ['--show-default-body'])
    assert result.exit_code == 0
    assert clusterjob.cli.DEFAULT_TEST_BODY in result.output

    monkeypatch.setattr(clusterjob.cli, '_run_testing_workflow', Mock())
    monkeypatch.setattr(clusterjob.cli, '_abort_pause', click.echo)
    monkeypatch.setattr(clusterjob.cli, 'write_file', Mock())
    monkeypatch.setattr(click, 'clear', Mock())
    monkeypatch.setattr(click, 'edit', Mock())
    monkeypatch.setattr(click, 'echo_via_pager', Mock())
    monkeypatch.setattr(click, 'pause', click.echo)
    monkeypatch.setattr(click, 'confirm', click.echo)
    inifile = os.path.join(test_dir, 'sge_local.ini')
    bodyfile = os.path.join(test_dir, 'sge_local.body.sh')
    result = runner.invoke(cli_backend_tester, ['--jobname', 'XXX',
                 '--body', bodyfile, inifile])
    assert result.exit_code == 0
    assert clusterjob.cli._run_testing_workflow.call_count == 1
    job = clusterjob.cli._run_testing_workflow.call_args[0][0]
    assert job.resources['jobname'] == 'XXX'
    assert job.resources['time'] == '00:05:00'
    assert job.resources['-j'] == 'y'
    assert job.resources['-S'] == '/bin/bash'
    assert job.resources['stdout'] == 'clusterjob_test.out'

    result = runner.invoke(cli_backend_tester, [inifile, ])
    assert result.exit_code == 0
    assert "Using default body" in result.output
    job = clusterjob.cli._run_testing_workflow.call_args[0][0]
    assert job.resources['jobname'] == 'test_clj'

    monkeypatch.syspath_prepend(test_dir)

    result = runner.invoke(cli_backend_tester, [inifile,
                    '--backend', 'custom_backends.Backend1'])
    assert result.exit_code == 1
    assert "backend must be an instance of ClusterjobBackend" in result.output

    result = runner.invoke(cli_backend_tester, [inifile,
                    '--backend', 'custom_backends.Backend2'])
    assert result.exit_code == 1
    assert "Can't instantiate abstract class Backend2 with abstract methods " \
            "get_status" in result.output

    result = runner.invoke(cli_backend_tester, [inifile,
                    '--backend', 'custom_backends.Backend3'])
    assert result.exit_code == 1
    assert "has no attribute 'name'" in result.output

    result = runner.invoke(cli_backend_tester, [inifile,
                    '--backend', 'custom_backends.Backend4'])
    assert result.exit_code == 0
    del job._backends['backend4']
