import pathlib
import unittest
from unittest.mock import patch

import pypers.pipeline
import pypers.task
from . import testsuite


class Task__init(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_without_parent(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertEqual(
            task.spec,
            dict(),
        )
        self.assertIsNone(task.parent)

    @testsuite.with_temporary_paths(2)
    def test_with_parent(self, path1, path2):
        parent = pypers.task.Task(
            path = path1,
            parent = None,
            spec = dict(),
        )
        task = pypers.task.Task(
            path = path2,
            parent = parent,
            spec = dict(),
        )
        self.assertIs(task.parent, parent)


class Task__full_spec(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_without_parent(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                field1 = 1,
                field2 = 2,
            ),
        )
        self.assertEqual(
            task.full_spec,
            dict(
                field1 = 1,
                field2 = 2,
            ),
        )

    @testsuite.with_temporary_paths(2)
    def test_with_parent(self, path1, path2):
        parent = pypers.task.Task(
            path = path1,
            parent = None,
            spec = dict(
                field1 = 1,
                field2 = 2,
            ),
        )
        task = pypers.task.Task(
            path = path2,
            parent = parent,
            spec = dict(
                field2 = 3,
                field3 = 4,
            ),
        )
        self.assertEqual(
            task.full_spec,
            dict(
                field1 = 1,
                field2 = 3,
                field3 = 4,
            ),
        )


class Task__create_pipeline(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_from_spec(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                pipeline = 'pypers.pipeline.Pipeline',
            ),
        )
        self.assertIsInstance(
            task.create_pipeline(),
            pypers.pipeline.Pipeline,
        )

    @testsuite.with_temporary_paths(1)
    @patch('pypers.pipeline.Pipeline')
    def test_from_spec_args_and_kwargs(self, path, mock_Pipeline):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                pipeline = 'pypers.pipeline.Pipeline',
            ),
        )
        task.create_pipeline(
            'arg1',
            'arg2',
            kwarg1 = 'kwarg1',
            kwarg2 = 'kwarg2',
        )
        mock_Pipeline.assert_called_once_with(
            'arg1',
            'arg2',
            kwarg1 = 'kwarg1',
            kwarg2 = 'kwarg2',
        )

    @testsuite.with_temporary_paths(1)
    def test_from_spec_missing(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        with self.assertRaises(AssertionError):
            task.create_pipeline()

    @testsuite.with_temporary_paths(1)
    def test_override(self, path):

        class DerivedTask(pypers.task.Task):

            def create_pipeline(self, *args, **kwargs):
                return pypers.pipeline.Pipeline(*args, **kwargs)
        
        task = DerivedTask(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertIsInstance(
            task.create_pipeline(),
            pypers.pipeline.Pipeline,
        )


def create_task_file(task_path, spec_yaml):
    task_path = pathlib.Path(task_path)
    task_filepath = task_path / 'task.yml'
    if not task_path.is_dir():
        task_path.mkdir(parents = True, exist_ok = True)
    with task_filepath.open('w') as spec_file:
        spec_file.write(spec_yaml)


class Batch__task(unittest.TestCase):

    def test_virtual_paths(self):
        batch = pypers.task.Batch()

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
        batch = pypers.task.Batch()
        task = batch.task(
            path = '',
            spec = dict(),
        )
        self.assertIsNone(task.parent)

    @testsuite.with_temporary_paths(1)
    def test_spec_files(self, root):
        create_task_file(root, 'pipeline: pypers.pipeline.Pipeline')
        create_task_file(root / 'task-2', 'field1: value1')

        # Load the tasks
        batch = pypers.task.Batch()
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
        create_task_file(root, 'pipeline: pypers.pipeline.Pipeline')
        batch = pypers.task.Batch()
        task = batch.task(
            path = root,
            spec = dict(pipeline = 'pypers.pipeline.Pipeline2'),
        )
        self.assertIsNone(task.parent)
        self.assertEqual(task.spec, dict(pipeline = 'pypers.pipeline.Pipeline2'))

    @testsuite.with_temporary_paths(1)
    def test_mixed_virtual_paths_and_spec_files(self, root):
        create_task_file(root, 'pipeline: pypers.pipeline.Pipeline')
        create_task_file(root / 'task-2', 'field1: value1')

        # Load the tasks
        batch = pypers.task.Batch()
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