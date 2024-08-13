import os
import pathlib
import pprint
import tempfile
import unittest

import pypers.batch
import pypers.pipeline
import pypers.task
from . import testsuite
import pypers.status


class Batch__task(unittest.TestCase):

    def test_virtual_paths(self):
        batch = pypers.batch.Batch()

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
        batch = pypers.batch.Batch()
        task = batch.task(
            path = '',
            spec = dict(),
        )
        self.assertIsNone(task.parent)

    @testsuite.with_temporary_paths(1)
    def test_spec_files(self, root):
        testsuite.create_task_file(root, 'pipeline: pypers.pipeline.Pipeline')
        testsuite.create_task_file(root / 'task-2', 'field1: value1')

        # Load the tasks
        batch = pypers.batch.Batch()
        task1 = batch.task(path = root)
        task2 = batch.task(path = root / 'task-2')

        # Verify the number of loaded tasks
        self.assertEqual(len(batch.tasks), 2)

        # Verify task1
        self.assertIsNone(task1.parent)
        self.assertEqual(task1.full_spec, dict(pipeline = 'pypers.pipeline.Pipeline'))

        # Verify task2
        self.assertIs(task2.parent, task1)
        self.assertEqual(task2.full_spec, dict(pipeline = 'pypers.pipeline.Pipeline', field1 = 'value1'))

    @testsuite.with_temporary_paths(1)
    def test_spec_files_with_override(self, root):
        testsuite.create_task_file(root, 'pipeline: pypers.pipeline.Pipeline')
        batch = pypers.batch.Batch()
        task = batch.task(
            path = root,
            spec = dict(pipeline = 'pypers.pipeline.Pipeline2'),
        )
        self.assertIsNone(task.parent)
        self.assertEqual(task.spec, dict(pipeline = 'pypers.pipeline.Pipeline2'))

    @testsuite.with_temporary_paths(1)
    def test_mixed_virtual_paths_and_spec_files(self, root):
        testsuite.create_task_file(root, 'pipeline: pypers.pipeline.Pipeline')
        testsuite.create_task_file(root / 'task-2', 'field1: value1')

        # Load the tasks
        batch = pypers.batch.Batch()
        task3 = batch.task(
            path = root / 'task-2' / 'task-3',
            spec = dict(field2 = 'value2'),
        )
        task2 = batch.task(path = root / 'task-2')

        # Verify the number of loaded tasks
        self.assertEqual(len(batch.tasks), 3)

        # Verify task3
        self.assertIs(task3.parent, task2)
        self.assertEqual(task3.full_spec, dict(pipeline = 'pypers.pipeline.Pipeline', field1 = 'value1', field2 = 'value2'))


class Batch__load(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root_path = pathlib.Path(self.tempdir.name)
        testsuite.create_task_file(self.root_path, 'pipeline: pypers.pipeline.Pipeline')
        testsuite.create_task_file(self.root_path / 'task-2', 'field1: value1')
        testsuite.create_task_file(self.root_path / 'task-2' / 'task-3', 'field2: value2')
        self.batch = pypers.batch.Batch()

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


class Batch__run(unittest.TestCase):

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
        self.batch = pypers.batch.Batch()
        self.batch.load(self.root_path)
        self.testsuite_pid = os.getpid()

    def tearDown(self):
        if os.getpid() == self.testsuite_pid:
            self.tempdir.cleanup()

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        status = pypers.status.Status(path = path)
        self.batch.run(status = status)
        self.assertEqual([list(item.keys()) for item in status.data], [['expand']] * 3, '\n' + pprint.pformat(status.data))