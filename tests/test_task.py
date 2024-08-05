import unittest
from unittest.mock import patch

import pypers.pipeline
import pypers.task


class Task__init(unittest.TestCase):

    def test_without_parent(self):
        task = pypers.task.Task(
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

    def test_with_parent(self):
        parent = pypers.task.Task(
            parent = None,
            spec = dict(
                field1 = 1,
                field2 = 2,
            ),
        )
        task = pypers.task.Task(
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

    def test_from_spec(self):
        task = pypers.task.Task(
            parent = None,
            spec = dict(
                pipeline = 'pypers.pipeline.Pipeline',
            ),
        )
        self.assertIsInstance(
            task.create_pipeline(),
            pypers.pipeline.Pipeline,
        )

    @patch('pypers.pipeline.Pipeline')
    def test_from_spec_args_and_kwargs(self, mock_Pipeline):
        task = pypers.task.Task(
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

    def test_from_spec_missing(self):
        task = pypers.task.Task(
            parent = None,
            spec = dict(),
        )
        with self.assertRaises(AssertionError):
            task.create_pipeline()

    def test_override(self):

        class DerivedTask(pypers.task.Task):

            def create_pipeline(self, *args, **kwargs):
                return pypers.pipeline.Pipeline(*args, **kwargs)
        
        task = DerivedTask(
            parent = None,
            spec = dict(),
        )
        self.assertIsInstance(
            task.create_pipeline(),
            pypers.pipeline.Pipeline,
        )