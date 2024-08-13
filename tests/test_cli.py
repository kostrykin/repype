import contextlib
import io
import os
import re
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import pypers.cli
from . import testsuite


class run_cli_ex(unittest.TestCase):

    stage1_cls = testsuite.create_stage_class(id = 'stage1')
    stage2_cls = testsuite.create_stage_class(id = 'stage2')

    def setUp(self):
        self.stdout_buf = io.StringIO()
        self.ctx = contextlib.redirect_stdout(self.stdout_buf)
        self.ctx.__enter__()

        self.tempdir = tempfile.TemporaryDirectory()
        self.root_path = pathlib.Path(self.tempdir.name)
        testsuite.create_task_file(
            self.root_path,
            'runnable: true' '\n'
            'pipeline:' '\n'
            '- tests.test_task.Task__create_pipeline.stage1_cls' '\n'
            '- tests.test_task.Task__create_pipeline.stage2_cls' '\n'
        )
        testsuite.create_task_file(
            self.root_path / 'task-2',
            'stage1:' '\n'
            '  key1: value1' '\n'
        )
        testsuite.create_task_file(
            self.root_path / 'task-3',
            'stage2:' '\n'
            '  key2: value2' '\n'
        )
        self.testsuite_pid = os.getpid()

    def tearDown(self):
        if os.getpid() == self.testsuite_pid:
            self.ctx.__exit__(None, None, None)
            self.tempdir.cleanup()

    @property
    def stdout(self):
        return re.sub(r'\033\[K', '', self.stdout_buf.getvalue())

    @patch.object(pypers.batch.Batch, 'run')
    def test(self, mock_batch_run):
        ret = pypers.cli.run_cli_ex(path = self.tempdir.name)
        self.assertTrue(ret)
        mock_batch_run.assert_not_called()
        self.assertEqual(
            self.stdout,
            '\n'
            '3 task(s) selected for running' '\n'
            'DRY RUN: use "--run" to run the tasks instead' '\n',
        )

    @patch.object(pypers.batch.Batch, 'run')
    def test_run(self, mock_batch_run):
        ret = pypers.cli.run_cli_ex(path = self.tempdir.name, run = True)
        self.assertTrue(ret)
        mock_batch_run.assert_called_once()
        self.assertIn('status', mock_batch_run.call_args_list[0].kwargs)
        self.assertEqual(len(mock_batch_run.call_args_list[0].args), 1)
        self.assertEqual([type(rc) for rc in mock_batch_run.call_args_list[0].args[0]], [pypers.batch.RunContext] * 3)
        self.assertEqual(
            self.stdout,
            '\n'
            '3 task(s) selected for running' '\n',
        )