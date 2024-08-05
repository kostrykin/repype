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
            spec = dict(
                field1 = 1,
                field2 = 2,
            ),
        )
        self.assertEqual(
            task.spec,
            dict(
                field1 = 1,
                field2 = 2,
            ),
        )

    @testsuite.with_temporary_paths(2)
    def test_without_parent(self, path1, path2):
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
            task.spec,
            dict(
                field1 = 1,
                field2 = 3,
                field3 = 4,
            ),
        )
        self.assertEqual(
            task.parent,
            parent,
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