import gzip
import json
import os
import pathlib
import tempfile
import unittest
from unittest.mock import (
    PropertyMock,
    patch,
)

import dill
import yaml

import repype.benchmark
import repype.pipeline
import repype.status
import repype.task

from . import testsuite


class decode_input_ids(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(repype.task.decode_input_ids(''), [])

    def test_single_str(self):
        self.assertEqual(repype.task.decode_input_ids('abc, xyz'), ['abc', 'xyz'])

    def test_single_int(self):
        self.assertEqual(repype.task.decode_input_ids(1), [1])

    def test_range(self):
        self.assertEqual(repype.task.decode_input_ids('1-2'), [1, 2])
        self.assertEqual(repype.task.decode_input_ids('1 - 3'), [1, 2, 3])

    def test_invalid(self):
        with self.assertRaises(ValueError):
            repype.task.decode_input_ids('1-')
        with self.assertRaises(ValueError):
            repype.task.decode_input_ids('-1')
        with self.assertRaises(ValueError):
            repype.task.decode_input_ids('-')
        with self.assertRaises(ValueError):
            repype.task.decode_input_ids('3-1')
        with self.assertRaises(ValueError):
            repype.task.decode_input_ids('1-1')

    def test_mixed(self):
        self.assertEqual(repype.task.decode_input_ids('1,2-3,4'), [1, 2, 3, 4])
        self.assertEqual(repype.task.decode_input_ids('1, 2-3'), [1, 2, 3])


class Task__init(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_with_path(self, path):
        task = repype.task.Task(
            path = pathlib.Path(path),
            parent = None,
            spec = dict(),
        )
        self.assertEqual(task.path, pathlib.Path(path))

    @testsuite.with_temporary_paths(1)
    def test_with_str_path(self, path):
        task = repype.task.Task(
            path = str(path),
            parent = None,
            spec = dict(),
        )
        self.assertEqual(task.path, pathlib.Path(path))

    @testsuite.with_temporary_paths(1)
    def test_without_parent(self, path):
        task = repype.task.Task(
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
        parent = repype.task.Task(
            path = path1,
            parent = None,
            spec = dict(),
        )
        task = repype.task.Task(
            path = path2,
            parent = parent,
            spec = dict(),
        )
        self.assertIs(task.parent, parent)


class Task__full_spec(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_without_parent(self, path):
        task = repype.task.Task(
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

    @testsuite.with_temporary_paths(3)
    def test_with_parent(self, path1, path2, path3):
        task1 = repype.task.Task(
            path = path1,
            parent = None,
            spec = dict(
                field1 = 1,
                field2 = 2,
            ),
        )
        task2 = repype.task.Task(
            path = path2,
            parent = task1,
            spec = dict(
                field2 = 3,
                field3 = 4,
                scopes = dict(
                    scope1 = 'scope1',
                )
            ),
        )
        task3 = repype.task.Task(
            path = path3,
            parent = task2,
            spec = dict(
                field2 = 5,
                scopes = dict(
                    scope2 = 'scope2',
                )
            ),
        )
        self.assertEqual(
            task2.full_spec,
            dict(
                field1 = 1,
                field2 = 3,
                field3 = 4,
                scopes = dict(
                    scope1 = 'scope1',
                )
            ),
        )
        self.assertEqual(
            task3.full_spec,
            dict(
                field1 = 1,
                field2 = 5,
                field3 = 4,
                scopes = dict(
                    scope1 = 'scope1',
                    scope2 = 'scope2',
                )
            ),
        )


class Task__repr__(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                field1 = 1,
                field2 = 2,
            ),
        )
        self.assertEqual(
            repr(task),
            f'<Task "{path}" bf21a9e>',
        )


class Task__create_pipeline(unittest.TestCase):

    def test_undefined(self):
        task = repype.task.Task(
            path = '',
            parent = None,
            spec = dict(),
        )
        with self.assertRaises(AssertionError):
            task.create_pipeline()

    @testsuite.with_temporary_paths(2)
    def test_from_spec(self, path1, path2):
        task = repype.task.Task(
            path = path1,
            parent = None,
            spec = dict(
                pipeline = 'repype.pipeline.Pipeline',
                scopes = dict(
                    inputs = str(path2 / 'inputs'),
                    outputs1 = 'outputs1',
                    outputs2 = '{ROOTDIR}/outputs2',
                ),
            ),
        )
        pipeline = task.create_pipeline()
        self.assertIsInstance(pipeline, repype.pipeline.Pipeline)
        self.assertEqual(
            pipeline.scopes,
            dict(
                inputs = path2.resolve() / 'inputs',
                outputs1 = task.path.resolve() / 'outputs1',
                outputs2 = task.root.path.resolve() / 'outputs2',
            ),
        )

    @testsuite.with_temporary_paths(1)
    @patch('repype.pipeline.Pipeline')
    def test_from_spec_args_and_kwargs(self, path, mock_Pipeline):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                pipeline = 'repype.pipeline.Pipeline',
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
            scopes = dict(),
            kwarg1 = 'kwarg1',
            kwarg2 = 'kwarg2',
        )

    @testsuite.with_temporary_paths(1)
    def test_from_spec_missing(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        with self.assertRaises(AssertionError):
            task.create_pipeline()

    @testsuite.with_temporary_paths(1)
    def test_override(self, path):
        class DerivedTask(repype.task.Task):

            def create_pipeline(self, *args, **kwargs):
                return repype.pipeline.Pipeline(*args, **kwargs)
        
        task = DerivedTask(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertIsInstance(
            task.create_pipeline(),
            repype.pipeline.Pipeline,
        )

    stage1_cls = testsuite.create_stage_class(id = 'stage1')
    stage2_cls = testsuite.create_stage_class(id = 'stage2')

    @testsuite.with_temporary_paths(1)
    def test_from_spec_with_stages(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                pipeline = [
                    'tests.test_task.Task__create_pipeline.stage1_cls',
                    'tests.test_task.Task__create_pipeline.stage2_cls',
                ],
            ),
        )
        self.assertIsInstance(task.create_pipeline(), repype.pipeline.Pipeline)
        self.assertEqual(frozenset([stage.id for stage in task.create_pipeline().stages]), {'stage1', 'stage2'})


class Task__create_config(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        # Verify that changes to the config do not affect the spec or full spec (if a config is defined)
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                config = dict(key1 = dict(key2 = 'value2')),
            ),
        )
        config = task.create_config()
        config['key1/key3'] = 'value3'
        self.assertEqual(task.spec, dict(config = dict(key1 = dict(key2 = 'value2'))))
        self.assertEqual(task.full_spec, dict(config = dict(key1 = dict(key2 = 'value2'))))

    @testsuite.with_temporary_paths(4)
    def test_with_base_config_path(self, task1_path, task2_path, task3_path, aux_path):
        task1 = repype.task.Task(
            path = task1_path,
            parent = None,
            spec = dict(
                config = dict(key1 = 'value1', key2 = 'value2', key3 = 'value3'),
            ),
        )
        base_config_path = aux_path / 'base_config.yml'
        with base_config_path.open('w') as spec_file:
            spec_file.write(
                'key1: value10' '\n'
                'key2: value20'
            )
        task2 = repype.task.Task(
            path = task2_path,
            parent = task1,
            spec = dict(
                base_config_path = base_config_path,
                config = dict(key2 = 'value200'),
            ),
        )
        task3 = repype.task.Task(
            path = task3_path,
            parent = task2,
            spec = dict(
                config = dict(key4 = 'value4'),
            ),
        )
        config = task3.create_config()
        self.assertEqual(config, repype.config.Config(dict(key1 = 'value10', key2 = 'value200', key3 = 'value3', key4 = 'value4')))


class Task__input_ids(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_str(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                input_ids = '1, 2-3, 4',
            ),
        )
        self.assertEqual(task.input_ids, [1, 2, 3, 4])

    @testsuite.with_temporary_paths(1)
    def test_list(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                input_ids = [
                    'id-1',
                    'id-2',
                ],
            ),
        )
        self.assertEqual(task.input_ids, ['id-1', 'id-2'])


class Task__root(unittest.TestCase):

    @testsuite.with_temporary_paths(3)
    def test(self, path1, path2, path3):
        task1 = repype.task.Task(
            path = path1,
            parent = None,
            spec = dict(),
        )
        task2 = repype.task.Task(
            path = path2,
            parent = task1,
            spec = dict(),
        )
        task3 = repype.task.Task(
            path = path3,
            parent = task2,
            spec = dict(),
        )
        self.assertIs(task1.root, task1)
        self.assertIs(task2.root, task1)
        self.assertIs(task3.root, task1)


class Task__resolve_path(unittest.TestCase):

    @testsuite.with_temporary_paths(2)
    def test_absolute(self, path1, path2):
        task1 = repype.task.Task(
            path = path1,
            parent = None,
            spec = dict(),
        )
        task2 = repype.task.Task(
            path = path2 / 'subdir',
            parent = task1,
            spec = dict(),
        )
        self.assertEqual(task2.resolve_path('file.txt'), (path2 / 'subdir' / 'file.txt').resolve())
        self.assertEqual(task2.resolve_path('./file.txt'), (path2 / 'subdir' / 'file.txt').resolve())
        self.assertEqual(task2.resolve_path('../file.txt'), (path2 / 'file.txt').resolve())
        self.assertEqual(task2.resolve_path('../{DIRNAME}.txt'), (path2 / 'subdir.txt').resolve())
        self.assertEqual(task2.resolve_path('{ROOTDIR}/file.txt'), (path1 / 'file.txt').resolve())
        self.assertEqual(task2.resolve_path('{ROOTDIR}/{DIRNAME}.txt'), (path1 / 'subdir.txt').resolve())

    @testsuite.with_temporary_paths(1)
    def test_relative(self, path):
        cwd = os.getcwd()
        os.chdir(path)
        try:
            os.makedirs('task1/task2')
            task1 = repype.task.Task(
                path = 'task1',
                parent = None,
                spec = dict(),
            )
            task2 = repype.task.Task(
                path = 'task1/task2',
                parent = task1,
                spec = dict(),
            )
            self.assertEqual(task2.resolve_path('file.txt'), (task2.path / 'file.txt').resolve())
            self.assertEqual(task2.resolve_path('./file.txt'), (task2.path / 'file.txt').resolve())
            self.assertEqual(task2.resolve_path('../file.txt'), (task1.path / 'file.txt').resolve())
            self.assertEqual(task2.resolve_path('../{DIRNAME}.txt'), (task1.path / 'task2.txt').resolve())
            self.assertEqual(task2.resolve_path('{ROOTDIR}/file.txt'), (task1.path / 'file.txt').resolve())
            self.assertEqual(task2.resolve_path('{ROOTDIR}/{DIRNAME}.txt'), (task1.path / 'task2.txt').resolve())
        finally:
            os.chdir(cwd)


class Task__is_pending(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.create_pipeline(
            [
                testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
            ]
        )

    @testsuite.with_temporary_paths(1)
    def test_not_runnable(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        config = task.create_config()
        self.assertEqual(task.is_pending(self.pipeline, config), '')

    @testsuite.with_temporary_paths(1)
    def test_without_digest(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        config = task.create_config()
        self.assertEqual(task.is_pending(self.pipeline, config), 'incomplete')

    @testsuite.with_temporary_paths(1)
    def test_with_digest(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        config = task.create_config()
        config['key'] = 'value'
        with task.resolve_path('.sha.json').open('w') as digest_sha_file:
            json.dump(
                dict(
                    stages = dict(
                        stage1 = self.pipeline.stages[0].sha,
                    ),
                    task = task.compute_sha(config),
                ),
                digest_sha_file,
            )
        self.assertEqual(task.is_pending(self.pipeline, config), '')

    @testsuite.with_temporary_paths(1)
    def test_with_changed_config(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        config = task.create_config()
        with task.resolve_path('.sha.json').open('w') as digest_sha_file:
            json.dump(
                dict(
                    stages = dict(
                        stage1 = self.pipeline.stages[0].sha,
                    ),
                    task = task.compute_sha(config),
                ),
                digest_sha_file,
            )
        config['key'] = 'value'
        self.assertEqual(task.is_pending(self.pipeline, config), 'specification')

    @testsuite.with_temporary_paths(1)
    def test_with_changed_pipeline(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        config = task.create_config()
        with task.resolve_path('.sha.json').open('w') as digest_sha_file:
            json.dump(
                dict(
                    stages = dict(
                        stage1 = self.pipeline.stages[0].sha[::-1],
                    ),
                    sha = task.compute_sha(config),
                ),
                digest_sha_file,
            )
        self.assertEqual(task.is_pending(self.pipeline, config), 'pipeline')


class Task__reset(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.create_pipeline(
            [
                testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
            ]
        )

    @testsuite.with_temporary_paths(1)
    def test_without_digest(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        task.reset()
        self.assertFalse(task. digest_sha_filepath.exists())
        self.assertFalse(task.digest_task_filepath.exists())
        self.assertFalse(task.       data_filepath.exists())

    @testsuite.with_temporary_paths(1)
    def test_with_digest(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        task. digest_sha_filepath.write_text('xxx')
        task.digest_task_filepath.write_text('xxx')
        task.       data_filepath.write_text('xxx')
        task.reset()
        self.assertFalse(task. digest_sha_filepath.exists())
        self.assertFalse(task.digest_task_filepath.exists())
        self.assertFalse(task.       data_filepath.exists())


class Task__marginal_states(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_from_spec_missing(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertEqual(list(task.marginal_stages), [])

    @testsuite.with_temporary_paths(1)
    def test_from_spec(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                marginal_stages = [
                    'stage1',
                    'stage2',
                ],
            ),
        )
        self.assertEqual(list(task.marginal_stages), ['stage1', 'stage2'])

    @testsuite.with_temporary_paths(1)
    def test_from_spec_class_names(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                marginal_stages = [
                    'tests.test_task.Task__create_pipeline.stage1_cls',
                    'tests.test_task.Task__create_pipeline.stage2_cls',
                ],
            ),
        )
        self.assertEqual(list(task.marginal_stages), ['stage1', 'stage2'])

    @testsuite.with_temporary_paths(1)
    def test_override(self, path):
        class DerivedTask(repype.task.Task):

            marginal_stages = [
                'stage1',
                'stage2',
            ]

        task = DerivedTask(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertEqual(list(task.marginal_stages), ['stage1', 'stage2'])


class Task__get_marginal_fields(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_empty_pipeline(self, path):
        task = repype.task.Task(
            path = '',
            parent = None,
            spec = dict(pipeline = 'repype.pipeline.Pipeline'),
        )
        self.assertEqual(task.get_marginal_fields(task.create_pipeline()), frozenset())

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        class DerivedTask(repype.task.Task):

            marginal_stages = [
                'stage1',
                'stage2',
            ]

            def create_pipeline(self, *args, **kwargs):
                stages = [
                    testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
                    testsuite.create_stage(id = 'stage2', outputs = ['output2.1', 'output2.2']),
                    testsuite.create_stage(id = 'stage3', outputs = ['output3']),
                ]
                return repype.pipeline.create_pipeline(stages, *args, **kwargs)
        
        task = DerivedTask(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertEqual(
            task.get_marginal_fields(task.create_pipeline()),
            frozenset(
                [
                    'output1.1',
                    'output2.1',
                    'output2.2',
                ]
            )
        )


class Task__store(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                runnable = True,
                input_ids = ['file-0'],
                marginal_stages = [
                    'stage2',
                ],
                config = dict(
                    key1 = 'value1',
                ),
            ),
        )
        pipeline = repype.pipeline.create_pipeline(
            [
                testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
                testsuite.create_stage(id = 'stage2', inputs = ['output1.1'], outputs = ['output2.1', 'output2.2']),
                testsuite.create_stage(id = 'stage3', inputs = ['output1.1', 'output2.1', 'output2.2'], outputs = ['output3.1']),
            ]
        )
        config = task.create_config()
        data = {
            'file-0': {
                'output1.1': 'value1.1',
                'output2.1': 'value2.1',
                'output2.2': 'value2.2',
                'output3.1': 'value3.1',
            },
        }
        times = repype.benchmark.Benchmark[float](task.times_filepath)
        times['stage1', 'file-0'] = 1.0
        times['stage2', 'file-0'] = 2.5
        times['stage3', 'file-0'] = 3.0
        times['stage4', 'file-0'] = 3.5
        task.store(pipeline, data, config, times)

        # Load the stored data
        with gzip.open(task.data_filepath, 'rb') as data_file:
            stored_data = dill.load(data_file)

        # Load the digest task specification
        with task.digest_task_filepath.open('r') as digest_task_file:
            task_digest = json.load(digest_task_file)

        self.assertFalse(task.is_pending(pipeline, config))
        self.assertEqual(
            stored_data,
            {
                'file-0': {
                    'output1.1': 'value1.1',
                    'output3.1': 'value3.1',
                }
            },
        )
        self.assertEqual(task_digest, task.full_spec)
        self.assertEqual(task.times, times)


class Task__load(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.task = repype.task.Task(
            path = pathlib.Path(self.tempdir.name),
            parent = None,
            spec = dict(
                runnable = True,
                input_ids = ['file-0'],
                marginal_stages = [
                    'stage2',
                ],
            ),
        )
        self.data_without_marginals = {
            'file-0': {
                'input_id': 'file-0',
                'output1.1': 'value1.1',
                'output3.1': 'value3.1',
            },
        }
        with gzip.open(self.task.data_filepath, 'wb') as data_file:
            dill.dump(self.data_without_marginals, data_file, byref=True)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_without_pipeline(self):
        data = self.task.load()
        self.assertEqual(data, self.data_without_marginals)

    def test_with_pipeline(self):
        pipeline = repype.pipeline.create_pipeline(
            [
                testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
                testsuite.create_stage(id = 'stage2', inputs = ['output1.1'], outputs = ['output2.1', 'output2.2']),
                testsuite.create_stage(id = 'stage3', inputs = ['output1.1', 'output2.1', 'output2.2'], outputs = ['output3.1']),
            ]
        )
        data = self.task.load(pipeline)
        self.assertEqual(data, self.data_without_marginals)


class Task__find_first_diverging_stage(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.task = repype.task.Task(
            path = pathlib.Path(self.tempdir.name),
            parent = None,
            spec = dict(
                runnable = True,
            ),
        )
        self.pipeline = repype.pipeline.create_pipeline(
            [
                testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
                testsuite.create_stage(id = 'stage2', inputs = ['output1.1'], outputs = ['output2.1', 'output2.2']),
                testsuite.create_stage(id = 'stage3', inputs = ['output1.1', 'output2.1', 'output2.2'], outputs = ['output3.1']),
            ]
        )
        self.config = self.task.create_config()
        with self.task.resolve_path('.sha.json').open('w') as digest_sha_file:
            json.dump(
                dict(
                    stages = dict(
                        stage1 = self.pipeline.stages[0].sha,
                        stage2 = self.pipeline.stages[1].sha,
                        stage3 = self.pipeline.stages[2].sha,
                    ),
                    task = self.task.compute_sha(self.config),
                ),
                digest_sha_file,
            )
        with self.task.resolve_path('.task.json').open('w') as digest_task_file:
            json.dump(
                self.task.get_full_spec_with_config(self.config),
                digest_task_file,
            )

    def tearDown(self):
        self.tempdir.cleanup()

    def test_unchanged(self):
        # There should be no divering stage if nothing is changed
        self.assertIsNone(
            self.task.find_first_diverging_stage(pipeline = self.pipeline, config = self.config),
        )

        # There should be no divering stage if a stage is replaced by the same stage
        self.pipeline.stages[1] = testsuite.create_stage(id = 'stage2', inputs = ['output1.1'], outputs = ['output2.1', 'output2.2'])
        self.assertIsNone(
            self.task.find_first_diverging_stage(pipeline = self.pipeline, config = self.config),
        )

    def test_removed_stage(self):
        self.pipeline.stages[1:] = self.pipeline.stages[2:]
        self.assertIsNone(
            self.task.find_first_diverging_stage(pipeline = self.pipeline, config = self.config),
        )

    def test_added_stage(self):
        self.pipeline.stages.append(
            testsuite.create_stage(id = 'stage4', inputs = ['output3.1'], outputs = ['output4.1'])
        )
        self.assertIs(
            self.task.find_first_diverging_stage(pipeline = self.pipeline, config = self.config),
            self.pipeline.stages[3],
        )

    def test_changed_stage(self):
        # Change the output of a stage
        self.pipeline.stages[1] = testsuite.create_stage(id = 'stage2', inputs = ['output1.1'], outputs = ['output2.1', 'output2.2', 'output2.3'])
        self.assertIs(
            self.task.find_first_diverging_stage(pipeline = self.pipeline, config = self.config),
            self.pipeline.stages[1],
        )

    def test_changed_config(self):
        self.config['stage2/key'] = 'value'
        self.assertIs(
            self.task.find_first_diverging_stage(pipeline = self.pipeline, config = self.config),
            self.pipeline.stages[1],
        )


class Task__find_pickup_task(unittest.TestCase):

    def setUp(self):
        self.tempdirs = [tempfile.TemporaryDirectory() for _ in range(3)]
        self.tasks = list()
        for tempdir in self.tempdirs:
            task = repype.task.Task(
                path = pathlib.Path(tempdir.name),
                parent = self.tasks[-1] if self.tasks else None,
                spec = dict(
                    runnable = True,
                ),
            )
            self.tasks.append(task)
        self.pipeline = repype.pipeline.create_pipeline(
            [
                testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
                testsuite.create_stage(id = 'stage2', inputs = ['output1.1'], outputs = ['output2.1', 'output2.2']),
                testsuite.create_stage(id = 'stage3', inputs = ['output1.1', 'output2.1', 'output2.2'], outputs = ['output3.1']),
            ]
        )
        self.configs = [task.create_config() for task in self.tasks]
                
    def _write_digests(self):
        for task, config in zip(self.tasks, self.configs):
            with task.resolve_path('.sha.json').open('w') as digest_sha_file:
                json.dump(
                    dict(
                        stages = dict(
                            stage1 = self.pipeline.stages[0].sha,
                            stage2 = self.pipeline.stages[1].sha,
                            stage3 = self.pipeline.stages[2].sha,
                        ),
                        task = task.compute_sha(config),
                    ),
                    digest_sha_file,
                )
            with task.resolve_path('.task.json').open('w') as digest_task_file:
                json.dump(
                    task.get_full_spec_with_config(config),
                    digest_task_file,
            )

    def tearDown(self):
        for tempdir in self.tempdirs:
            tempdir.cleanup()

    def test_task1_nothing_to_pickup_from(self):
        self.assertEqual(self.tasks[0].find_pickup_task(self.pipeline, self.configs[0]), dict(
            task = None,
            first_diverging_stage = self.pipeline.stages[0],
        ))

    def test_task2_nothing_to_pickup_from(self):
        self.configs[0]['stage1/key'] = 'value1.1'
        self._write_digests()
        self.configs[1]['stage1/key'] = 'value2.1'
        self.assertEqual(self.tasks[1].find_pickup_task(self.pipeline, self.configs[1]), dict(
            task = None,
            first_diverging_stage = self.pipeline.stages[0],
        ))

    def test_task1_pickup_from_task1(self):
        self.configs[0]['stage1/key'] = 'value1.1'
        self.configs[0]['stage2/key'] = 'value2.1'
        self._write_digests()
        self.configs[0]['stage2/key'] = 'value2.2'
        self.assertEqual(self.tasks[0].find_pickup_task(self.pipeline, self.configs[0]), dict(
            task = self.tasks[0],
            first_diverging_stage = self.pipeline.stages[1],
        ))

    def test_task1_pickup_from_task1_without_changes(self):
        self.configs[0]['stage1/key'] = 'value1.1'
        self.configs[0]['stage2/key'] = 'value2.1'
        self._write_digests()
        self.assertEqual(self.tasks[0].find_pickup_task(self.pipeline, self.configs[0]), dict(
            task = self.tasks[0],
            first_diverging_stage = None,
        ))

    def test_task2_pickup_from_task1(self):
        self.configs[0]['stage1/key'] = 'value1.1'
        self._write_digests()
        self.configs[1]['stage1/key'] = 'value1.1'
        self.configs[1]['stage2/key'] = 'value2.1'
        self.assertEqual(self.tasks[1].find_pickup_task(self.pipeline, self.configs[1]), dict(
            task = self.tasks[0],
            first_diverging_stage = self.pipeline.stages[1],
        ))

    def test_task2_pickup_from_task1_without_changes(self):
        self.configs[0]['stage1/key'] = 'value1.1'
        self._write_digests()
        self.configs[1]['stage1/key'] = 'value1.1'
        self.assertEqual(self.tasks[1].find_pickup_task(self.pipeline, self.configs[1]), dict(
            task = self.tasks[0],
            first_diverging_stage = None,
        ))

    def test_task3_pickup_from_task1(self):
        self.configs[0]['stage1/key'] = 'value1.1'
        self._write_digests()
        self.configs[2]['stage1/key'] = 'value1.1'
        self.configs[2]['stage2/key'] = 'value2.1'
        self.assertEqual(self.tasks[2].find_pickup_task(self.pipeline, self.configs[2]), dict(
            task = self.tasks[0],
            first_diverging_stage = self.pipeline.stages[1],
        ))

    def test_task3_pickup_from_task2(self):
        self.configs[0]['stage1/key'] = 'value1.1'
        self.configs[0]['stage2/key'] = 'value2.1'
        self.configs[1]['stage1/key'] = 'value1.2'
        self.configs[1]['stage2/key'] = 'value2.2'
        self._write_digests()
        self.configs[2]['stage1/key'] = 'value1.2'
        self.configs[2]['stage2/key'] = 'value2.3'
        self.assertEqual(self.tasks[2].find_pickup_task(self.pipeline, self.configs[2]), dict(
            task = self.tasks[1],
            first_diverging_stage = self.pipeline.stages[1],
        ))


@patch.object(repype.task.Task, 'store')
@patch.object(repype.task.Task, 'load')
@patch.object(repype.task.Task, 'create_pipeline')
class Task__run(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.task = repype.task.Task(
            path = pathlib.Path(self.tempdir.name),
            parent = None,
            spec = dict(
                runnable = True,
                input_ids = ['file-0', 'file-1'],
            ),
        )
        self.config = self.task.create_config()

    def tearDown(self):
        self.tempdir.cleanup()

    @patch.object(repype.task.Task, 'runnable', return_value = False, new_callable = PropertyMock)
    def test_not_runnable(self, *args):
        with self.assertRaises(AssertionError):
            self.task.run(self.config)

    def test_store_config(self, mock_create_pipeline, mock_load, mock_store):
        mock_create_pipeline.return_value.configure.return_value = dict(key = 'value')
        mock_create_pipeline.return_value.process.return_value = (dict(), None, dict())
        self.task.run(self.config)
        mock_store.assert_called_once()
        self.assertEqual(mock_store.call_args[0][2].entries, dict())

    def test_nothing_to_pickup(self, mock_create_pipeline, mock_load, mock_store):
        mock_create_pipeline.return_value.process.return_value = (dict(), None, dict())
        self.task.run(self.config)
        mock_load.assert_not_called()
        mock_create_pipeline.assert_called_once_with()
        mock_create_pipeline.return_value.process.assert_any_call(
            input_id = 'file-0',
            data = dict(),
            config = mock_create_pipeline.return_value.configure(self.config, 'file-0'),
            first_stage = None,
            status = None,
        )
        mock_create_pipeline.return_value.process.assert_any_call(
            input_id = 'file-1',
            data = dict(),
            config = mock_create_pipeline.return_value.configure(self.config, 'file-1'),
            first_stage = None,
            status = None,
        )
        self.assertEqual(mock_create_pipeline.return_value.process.call_count, 2)
        mock_store.assert_called_once()

    def test_with_pickup(self, mock_create_pipeline, mock_load, mock_store):
        mock_create_pipeline.return_value.process.return_value = (dict(), None, dict())
        mock_load.return_value = {
            'file-0': dict(output = 'value1'),
            'file-1': dict(output = 'value2'),
        }
        stage1 = testsuite.create_stage(id = 'stage-1')
        with patch.object(repype.task.Task, 'find_pickup_task', return_value = dict(task = self.task, first_diverging_stage = stage1)) as mock_find_pickup_task:
            self.task.run(self.config)
        mock_find_pickup_task.assert_called_once_with(
            mock_create_pipeline.return_value,
            self.config,
        )
        mock_load.assert_called_once()
        mock_create_pipeline.assert_called_once_with()
        mock_create_pipeline.return_value.process.assert_any_call(
            input_id = 'file-0',
            data = dict(output = 'value1'),
            config = mock_create_pipeline.return_value.configure(self.config, 'file-0'),
            first_stage = 'stage-1',
            status = None,
        )
        mock_create_pipeline.return_value.process.assert_any_call(
            input_id = 'file-1',
            data = dict(output = 'value2'),
            config = mock_create_pipeline.return_value.configure(self.config, 'file-1'),
            first_stage = 'stage-1',
            status = None,
        )
        self.assertEqual(mock_create_pipeline.return_value.process.call_count, 2)
        mock_store.assert_called_once()

    def test_pickup_with_copy(self, mock_create_pipeline, mock_load, mock_store):
        mock_create_pipeline.return_value.process.return_value = (dict(), None, dict())
        mock_load.return_value = {
            'file-0': dict(output = 'value1'),
            'file-1': dict(output = 'value2'),
        }
        with patch.object(repype.task.Task, 'find_pickup_task', return_value = dict(task = self.task, first_diverging_stage = None)) as mock_find_pickup_task:
            self.task.run(self.config)
        mock_find_pickup_task.assert_called_once_with(
            mock_create_pipeline.return_value,
            self.config,
        )
        mock_load.assert_called_once()
        mock_create_pipeline.assert_called_once_with()
        self.assertEqual(mock_create_pipeline.return_value.process.call_count, 0)
        mock_store.assert_called_once()


class Task__final_config(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        task = repype.task.Task(
            path = path,
            parent = None,
            spec = dict(
                runnable = True,
                pipeline = 'repype.pipeline.Pipeline',
                input_ids = [1, 2],
                scopes = dict(
                    config = 'cfg/%d.yml',
                ),
                config = dict(
                    stage1 = dict(
                        key1 = 'value1',
                        key2 = 'value2',
                    ),
                ),
            ),
        )
        pipeline = task.create_pipeline()
        pipeline.append(testsuite.create_stage(id = 'stage1'))
        task.run(task.create_config(), pipeline)
        for input_id in [1, 2]:
            with self.subTest(input_id = input_id):
                with (path / 'cfg' / f'{input_id}.yml').open('r') as config_file:
                    config = repype.config.Config(yaml.safe_load(config_file))
                    self.assertEqual(
                        config,
                        repype.config.Config(
                            dict(
                                stage1 = dict(
                                    enabled = True,
                                    key1 = 'value1',
                                    key2 = 'value2',
                                ),
                            ),
                        ),
                    )