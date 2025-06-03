import asyncio
import pathlib
import tempfile
import time
import unittest
from unittest.mock import patch

import repype.cli

from . import (
    test_status,
    testsuite,
)


class StatusReaderConsoleAdapter__write(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.status = repype.status.Status(path = self.tempdir.name)
        self.status_reader = repype.cli.StatusReaderConsoleAdapter(self.status.filepath)
        await self.status_reader.__aenter__()

    async def asyncTearDown(self):
        await self.status_reader.__aexit__(None, None, None)
        self.tempdir.cleanup()

    async def test(self):
        with testsuite.CaptureStdout() as stdout:
            self.status.write('message')
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), 'message\n')


class StatusReaderConsoleAdapter__intermediate(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.status = repype.status.Status(path = self.tempdir.name)
        self.status_reader = repype.cli.StatusReaderConsoleAdapter(self.status.filepath)
        await self.status_reader.__aenter__()

    async def asyncTearDown(self):
        await self.status_reader.__aexit__(None, None, None)
        self.tempdir.cleanup()

    async def test(self):
        with testsuite.CaptureStdout() as stdout:
            self.status.intermediate('message 1')
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), 'message 1\r')

            self.status.intermediate('message 2')
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), 'message 1\rmessage 2\r')

    @testsuite.with_envvars(REPYPE_CLI_INTERMEDIATE = '0')
    async def test_muted(self):
        with testsuite.CaptureStdout() as stdout:
            self.status.write('message 1')

            self.status.intermediate('message 2')
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), 'message 1\n')


class StatusReaderConsoleAdapter__progress(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.status = repype.status.Status(path = self.tempdir.name)
        self.status_reader = repype.cli.StatusReaderConsoleAdapter(self.status.filepath)
        await self.status_reader.__aenter__()

    async def asyncTearDown(self):
        if self.status_reader is not None:
            await self.status_reader.__aexit__(None, None, None)
        self.tempdir.cleanup()

    async def test(self):
        lines = [
            '[                    ] 0.0% (0 / 3)\r',
            '[======              ] 33.3% (1 / 3, ETA: 00:02)\r',
            '[=============       ] 66.7% (2 / 3, ETA: 00:01)\r',
            '                                                \r',
        ]
        with testsuite.CaptureStdout() as stdout:
            for item_idx, item in enumerate(repype.status.progress(self.status, range(3))):
                await asyncio.sleep(1)
                self.assertEqual(str(stdout), ''.join(lines[:item_idx + 1]))
        
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), ''.join(lines))

        # Verify that there have been three iterations, i.e. `item_idx = 0`, `item_idx = 1`, `item_idx = 2`
        self.assertEqual(item_idx, 2)

    async def test_with_details_str(self):
        lines = [
            'details [                    ] 0.0% (0 / 3)\r',
            'details [======              ] 33.3% (1 / 3, ETA: 00:02)\r',
            'details [=============       ] 66.7% (2 / 3, ETA: 00:01)\r',
            '                                                        \r',
        ]
        with testsuite.CaptureStdout() as stdout:
            for item_idx, item in enumerate(repype.status.progress(self.status, range(3), details = 'details')):
                await asyncio.sleep(1)
                self.assertEqual(str(stdout), ''.join(lines[:item_idx + 1]))
        
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), ''.join(lines))

        # Verify that there have been three iterations, i.e. `item_idx = 0`, `item_idx = 1`, `item_idx = 2`
        self.assertEqual(item_idx, 2)

    async def test_with_details_dict(self):
        lines = [
            "{'info': 'details'} [                    ] 0.0% (0 / 3)\r",
            "{'info': 'details'} [======              ] 33.3% (1 / 3, ETA: 00:02)\r",
            "{'info': 'details'} [=============       ] 66.7% (2 / 3, ETA: 00:01)\r",
            '                                                                    \r',
        ]
        with testsuite.CaptureStdout() as stdout:
            for item_idx, item in enumerate(repype.status.progress(self.status, range(3), details = dict(info = 'details'))):
                await asyncio.sleep(1)
                self.assertEqual(str(stdout), ''.join(lines[:item_idx + 1]))
        
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), ''.join(lines))

        # Verify that there have been three iterations, i.e. `item_idx = 0`, `item_idx = 1`, `item_idx = 2`
        self.assertEqual(item_idx, 2)

    async def test_break(self):
        lines = [
            "{'info': 'details'} [                    ] 0.0% (0 / 3)\r",
            "{'info': 'details'} [======              ] 33.3% (1 / 3, ETA: 00:02)\r",
            '                                                                    \r',
        ]
        with testsuite.CaptureStdout() as stdout:
            for item_idx, item in enumerate(repype.status.progress(self.status, range(3), details = dict(info = 'details'))):
                await asyncio.sleep(1)
                self.assertEqual(str(stdout), ''.join(lines[:item_idx + 1]))
                if item_idx == 1:
                    break
        
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), ''.join(lines))

        # Verify that there have been two iterations, i.e. `item_idx = 0`, `item_idx = 1`
        self.assertEqual(item_idx, 1)

    async def test_error(self):
        lines = [
            "{'info': 'details'} [                    ] 0.0% (0 / 3)\r",
            "{'info': 'details'} [======              ] 33.3% (1 / 3, ETA: 00:02)\r",
            '                                                                    \r',
        ]
        with testsuite.CaptureStdout() as stdout:
            with self.assertRaises(testsuite.TestError):
                for item_idx, item in enumerate(repype.status.progress(self.status, range(3), details = dict(info = 'details'))):
                    await asyncio.sleep(1)
                    self.assertEqual(str(stdout), ''.join(lines[:item_idx + 1]))
                    if item_idx == 1:
                        raise testsuite.TestError()
        
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), ''.join(lines))

        # Verify that there have been two iterations, i.e. `item_idx = 0`, `item_idx = 1`
        self.assertEqual(item_idx, 1)

    async def test_blocking(self):
        # Shutdown the default status reader of the test case
        await self.status_reader.__aexit__(None, None, None)
        self.status_reader = None

        # Suppress asyncio complaining about long-running co-routine to avoid cluttering the output
        import logging
        logging.getLogger('asyncio').setLevel(logging.ERROR)

        lines = [
            '[                    ] 0.0% (0 / 3)\r',
            '[======              ] 33.3% (1 / 3, ETA: 00:02)\r',
            '[=============       ] 66.7% (2 / 3, ETA: 00:01)\r',
            '                                                \r',
        ]
        with testsuite.CaptureStdout() as stdout:
            async with repype.cli.StatusReaderConsoleAdapter(self.status.filepath, blocking = True):
                for item_idx, item in enumerate(repype.status.progress(self.status, range(3))):
                    time.sleep(1)
                    self.assertEqual(str(stdout), ''.join(lines[:item_idx + 1]))
        
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), ''.join(lines))

    async def test_delayed(self):
        """
        Test that the status reader is delayed so that it misses the first step of the progress iteration.

        This is a regression test for a race-condition.
        """
        with testsuite.CaptureStdout():
            for step, _ in enumerate(repype.status.progress(self.status, range(4))):
                if step >= 2:
                    # Wait for the status reader to process the output, now that the first two steps have been missed
                    await test_status.wait_for_watchdog()


class ExtendedStatusReaderConsoleAdapter(repype.cli.StatusReaderConsoleAdapter):

    def format(self, positions, status, intermediate):

        if isinstance(status, dict) and status.get('info') == 'custom':
            return status['text']
        
        return super().format(positions, status, intermediate)


class ExtendedStatusReaderConsoleAdapter__format(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.status = repype.status.Status(path = self.tempdir.name)
        self.status_reader = ExtendedStatusReaderConsoleAdapter(self.status.filepath)
        await self.status_reader.__aenter__()

    async def asyncTearDown(self):
        await self.status_reader.__aexit__(None, None, None)
        self.tempdir.cleanup()

    async def test(self):
        with testsuite.CaptureStdout() as stdout:
            repype.status.update(self.status, info = 'custom', text = 'message')
            await test_status.wait_for_watchdog()
            self.assertEqual(str(stdout), 'message\n')


class DelayedTask(repype.task.Task):

    def store(self, *args, **kwargs):
        # Delay Task.store by 1 second, so that intermediates don't collapse too quickly
        time.sleep(1)
        return super().store(*args, **kwargs)
    

class DefectiveTask(repype.task.Task):

    def store(self, *args, **kwargs):
        time.sleep(1)  # Make sure the intermediates don't collapse too quickly
        raise testsuite.TestError()


class run_cli_ex(unittest.TestCase):

    stage1_cls = testsuite.create_stage_class(id = 'stage1', inputs = ['input'  ], outputs = ['output1'])
    stage2_cls = testsuite.create_stage_class(id = 'stage2', inputs = ['output1'], outputs = ['output2'])

    def setUp(self):
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
            'config:' '\n'
            '  stage1:' '\n'
            '    key1: value1' '\n'
        )
        testsuite.create_task_file(
            self.root_path / 'task-3',
            'config:' '\n'
            '  stage2:' '\n'
            '    key2: value2' '\n'
        )

    def tearDown(self):
        self.tempdir.cleanup()

    @patch.object(repype.batch.Batch, 'run')
    def test(self, mock_batch_run):
        with testsuite.CaptureStdout() as stdout:
            ret = repype.cli.run_cli_ex(path = self.tempdir.name)
            self.assertTrue(ret)
            mock_batch_run.assert_not_called()
            self.assertEqual(
                str(stdout),
                '\n'
                '3 task(s) selected for running' '\n'
                'DRY RUN: use "--run" to run the tasks instead' '\n'
                '\n'
                'Selected tasks:' '\n'
                f'- {self.root_path.resolve()} (incomplete)' '\n'
                f'- {self.root_path.resolve() / "task-2"} (incomplete)' '\n'
                f'- {self.root_path.resolve() / "task-3"} (incomplete)' '\n',
            )

    @patch.object(repype.batch.Batch, 'run')
    def test_run(self, mock_batch_run):
        with testsuite.CaptureStdout() as stdout:
            ret = repype.cli.run_cli_ex(path = self.tempdir.name, run = True)
            self.assertTrue(ret)
            mock_batch_run.assert_called_once()
            self.assertIn('status', mock_batch_run.call_args_list[0].kwargs)
            self.assertEqual(len(mock_batch_run.call_args_list[0].args), 1)
            self.assertEqual([type(rc) for rc in mock_batch_run.call_args_list[0].args[0]], [repype.batch.RunContext] * 3)
            self.assertEqual(
                str(stdout),
                '\n'
                '3 task(s) selected for running' '\n',
            )

    def test_run_integrated(self):
        with testsuite.CaptureStdout() as stdout:
            ret = repype.cli.run_cli_ex(path = self.tempdir.name, run = True, task_cls = DelayedTask)
            self.assertTrue(ret)
            self.assertEqual(
                str(stdout),
                f'\n'
                f'3 task(s) selected for running' '\n'
                f'  \n'
                f'  (1/3) Entering task: {self.root_path.resolve()}' '\n'
                f'  Starting from scratch' '\n'
                f'  Storing results...' '\r'
                f'  Results have been stored ✅' '\n'
                f'  \n'
                f'  (2/3) Entering task: {self.root_path.resolve()}/task-2' '\n'
                f'  Starting from scratch' '\n'
                f'  Storing results...' '\r'
                f'  Results have been stored ✅' '\n'
                f'  \n'
                f'  (3/3) Entering task: {self.root_path.resolve()}/task-3' '\n'
                f'  Picking up from: {self.root_path.resolve()} (stage2)' '\n'
                f'  Storing results...' '\r'
                f'  Results have been stored ✅' '\n'
            )

    def test_internal_error(self):
        with testsuite.CaptureStdout() as stdout:
            ret = repype.cli.run_cli_ex(path = self.tempdir.name, run = True, task_cls = DefectiveTask)
            self.assertFalse(ret)
            self.assertIn(
                f'\n'
                f'3 task(s) selected for running' '\n'
                f'  \n'
                f'  (1/3) Entering task: {self.root_path.resolve()}' '\n'
                f'  Starting from scratch' '\n'
                f'  Storing results...' '\r'
                f'                    ' '\n'
                f'  🔴 An error occurred while processing the task {self.root_path.resolve()}:' '\n'
                f'  --------------------------------------------------------------------------------' '\n'
                f'  Traceback (most recent call last):',
                str(stdout),
            )
            self.assertIn(
                f'  tests.testsuite.TestError' '\n'
                f'  --------------------------------------------------------------------------------' '\n'
                f'\n'
                f'🔴 Batch run interrupted' '\n',
                str(stdout),
            )