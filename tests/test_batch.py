import asyncio
import pathlib
import pprint
import tempfile
import time
import unittest

import repype.batch
import repype.pipeline
import repype.status
import repype.task

from . import (
    test_cli,
    testsuite,
)


class RunContext__eq__(unittest.TestCase):

    def setUp(self):
        self.task1 = repype.task.Task(
            path = 'task1',
            spec = dict(
                runnable = True,
                pipeline = 'repype.pipeline.Pipeline',
            ),
        )
        self.task2 = repype.task.Task(
            path = 'task1',
            spec = dict(
                runnable = True,
                pipeline = 'repype.pipeline.Pipeline',
            ),
        )
        self.rc1 = repype.batch.RunContext(self.task1)
        self.rc2 = repype.batch.RunContext(self.task2)

    def test__equality(self):
        self.assertEqual(self.rc1, self.rc1)
        self.assertEqual(self.rc1, self.rc2)

    def test__inequality(self):
        self.rc2.config['key'] = 'value'
        self.assertNotEqual(self.rc1, self.rc2)


class Batch__task(unittest.TestCase):

    def test_virtual_paths(self):
        batch = repype.batch.Batch()

        # Load the tasks
        task1 = batch.task(
            path = 'path/to/task',
            spec = dict(),
        )
        task2 = batch.task(
            path = 'path/to/task/2',
            spec = dict(),
        )

        # Verify the tasks
        self.assertEqual(len(batch.tasks), 2)
        self.assertIsNone(task1.parent)
        self.assertIs(task2.parent, task1)

    def test_virtual_path_without_parent(self):
        batch = repype.batch.Batch()
        task = batch.task(
            path = '',
            spec = dict(),
        )
        self.assertIsNone(task.parent)

    @testsuite.with_temporary_paths(1)
    def test_spec_files(self, root):
        testsuite.create_task_file(root, 'pipeline: repype.pipeline.Pipeline')
        testsuite.create_task_file(root / 'task-2', 'field1: value1')

        # Load the tasks
        batch = repype.batch.Batch()
        task1 = batch.task(path = root)
        task2 = batch.task(path = root / 'task-2')

        # Verify the number of loaded tasks
        self.assertEqual(len(batch.tasks), 2)

        # Verify task1
        self.assertIsNone(task1.parent)
        self.assertEqual(task1.full_spec, dict(pipeline = 'repype.pipeline.Pipeline'))

        # Verify task2
        self.assertIs(task2.parent, task1)
        self.assertEqual(task2.full_spec, dict(pipeline = 'repype.pipeline.Pipeline', field1 = 'value1'))

    @testsuite.with_temporary_paths(1)
    def test_spec_files_with_override(self, root):
        testsuite.create_task_file(root, 'pipeline: repype.pipeline.Pipeline')
        batch = repype.batch.Batch()
        task = batch.task(
            path = root,
            spec = dict(pipeline = 'repype.pipeline.Pipeline2'),
        )
        self.assertIsNone(task.parent)
        self.assertEqual(task.spec, dict(pipeline = 'repype.pipeline.Pipeline2'))

    @testsuite.with_temporary_paths(1)
    def test_mixed_virtual_paths_and_spec_files(self, root):
        testsuite.create_task_file(root, 'pipeline: repype.pipeline.Pipeline')
        testsuite.create_task_file(root / 'task-2', 'field1: value1')

        # Load the tasks
        batch = repype.batch.Batch()
        task3 = batch.task(
            path = root / 'task-2' / 'task-3',
            spec = dict(field2 = 'value2'),
        )
        task2 = batch.task(path = root / 'task-2')

        # Verify the number of loaded tasks
        self.assertEqual(len(batch.tasks), 3)

        # Verify task3
        self.assertIs(task3.parent, task2)
        self.assertEqual(task3.full_spec, dict(pipeline = 'repype.pipeline.Pipeline', field1 = 'value1', field2 = 'value2'))

    def test_path_identity(self):
        batch = repype.batch.Batch()
        task1 = batch.task(
            path = '.',
            spec = dict(),
        )
        self.assertIs(batch.task(task1.path.resolve()), task1)


class Batch__load(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root_path = pathlib.Path(self.tempdir.name)
        testsuite.create_task_file(self.root_path, 'pipeline: repype.pipeline.Pipeline')
        testsuite.create_task_file(self.root_path / 'task-2', 'field1: value1')
        testsuite.create_task_file(self.root_path / 'task-2' / 'task-3', 'field2: value2')
        self.batch = repype.batch.Batch()

    def tearDown(self):
        self.tempdir.cleanup()

    def test(self):
        self.batch.load(self.root_path)
        self.assertEqual(
            [task.path for task in self.batch.tasks.values()],
            [
                self.root_path,
                self.root_path / 'task-2',
                self.root_path / 'task-2' / 'task-3',
            ],
        )

    def test_illegal_path(self):
        with self.assertRaises(AssertionError):
            self.batch.load(self.root_path / 'task-3')


class Batch__contexts(unittest.TestCase):

    stage1_cls = testsuite.create_stage_class(id = 'stage1')
    stage2_cls = testsuite.create_stage_class(id = 'stage2')

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
        self.batch = repype.batch.Batch()
        self.batch.load(self.root_path)

    def tearDown(self):
        self.tempdir.cleanup()

    def test(self):
        contexts = self.batch.contexts
        self.assertEqual(len(contexts), 3)
        self.assertEqual(
            [context.task.path for context in contexts],
            [
                self.root_path,
                self.root_path / 'task-2',
                self.root_path / 'task-3',
            ],
        )
        self.assertEqual(
            [context.config.entries for context in contexts],
            [
                dict(),
                dict(stage1 = dict(key1 = 'value1')),
                dict(stage2 = dict(key2 = 'value2')),
            ],
        )


class Batch__pending(unittest.TestCase):

    def setUp(self):
        self.batch__contexts = Batch__contexts()
        self.batch__contexts.setUp()

    def tearDown(self):
        self.batch__contexts.tearDown()

    def test(self):
        self.assertEqual(
            [rc.task.path for rc in self.batch__contexts.batch.pending],
            [
                self.batch__contexts.root_path,
                self.batch__contexts.root_path / 'task-2',
                self.batch__contexts.root_path / 'task-3',
            ],
        )

    def test_after_run(self):
        asyncio.run(self.batch__contexts.batch.run())
        self.assertEqual(
            [rc.task.path for rc in self.batch__contexts.batch.pending],
            [
            ],
        )

    def test_after_run_partial(self):
        contexts = self.batch__contexts.batch.pending[:1]
        asyncio.run(self.batch__contexts.batch.run(contexts))
        self.assertEqual(
            [rc.task.path for rc in self.batch__contexts.batch.pending],
            [
                self.batch__contexts.root_path / 'task-2',
                self.batch__contexts.root_path / 'task-3',
            ],
        )


class Batch__run(unittest.IsolatedAsyncioTestCase):

    stage1_cls = testsuite.create_stage_class(id = 'stage1')
    stage2_cls = testsuite.create_stage_class(id = 'stage2')

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
            'stage1:' '\n'
            '  key1: value1' '\n'
        )
        testsuite.create_task_file(
            self.root_path / 'task-3',
            'stage2:' '\n'
            '  key2: value2' '\n'
        )
        self.batch = repype.batch.Batch()
        self.batch.load(self.root_path)

    def tearDown(self):
        self.tempdir.cleanup()

    @testsuite.with_temporary_paths(1)
    async def test(self, path):
        status = repype.status.Status(path = path)
        ret = await self.batch.run(status = status)
        self.assertTrue(ret)
        self.assertEqual([list(item.keys()) for item in status.data], [['expand']] * 3, '\n' + pprint.pformat(status.data))

    @unittest.mock.patch('dill.dumps', side_effect = lambda args: args)
    @unittest.mock.patch('asyncio.create_subprocess_exec', new_callable = unittest.mock.AsyncMock)
    @unittest.mock.patch('repype.status')
    async def test_wrong_order(self, mock_status, mock_create_subprocess_exec, mock_dill_dumps):
        mock_task_process = await mock_create_subprocess_exec()
        mock_task_process.communicate.return_value = ('0', None)

        rc1 = self.batch.context(self.root_path)
        rc2 = self.batch.context(self.root_path / 'task-2')
        rc3 = self.batch.context(self.root_path / 'task-3')
        ret = await self.batch.run([rc2, rc1, rc3])

        call_order = [call.kwargs['input'][0] for call in mock_task_process.communicate.call_args_list]
        self.assertEqual(call_order, [rc1, rc2, rc3])
        self.assertTrue(ret)


class Batch__cancel(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.batch__run = Batch__run()
        self.batch__run.setUp()
        self.batch = repype.batch.Batch(task_cls = test_cli.DelayedTask)
        self.batch.load(self.batch__run.root_path)

    def tearDown(self):
        self.batch__run.tearDown()

    @testsuite.with_temporary_paths(1)
    async def test(self, path):
        # Start the run, but do not await
        t0 = time.time()
        status = repype.status.Status(path = path)
        batch_run = asyncio.create_task(self.batch.run(status = status))

        # Do the cancellation after 0.5 second
        await asyncio.sleep(0.5)
        await self.batch.cancel()

        # Wait for the run to finish
        ret = await batch_run
        dt = time.time() - t0

        # Verify the results
        self.assertAlmostEqual(dt, 0.5, delta = 0.15)
        self.assertFalse(ret)
        self.assertIn(
            dict(
                info = 'interrupted',
                exit_code = None,
            ),
            status.data,
        )