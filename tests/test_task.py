import dill
import gzip
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import pypers.pipeline
import pypers.task
from . import testsuite


class decode_file_ids(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(pypers.task.decode_file_ids(''), [])

    def test_single(self):
        self.assertEqual(pypers.task.decode_file_ids('1'), [1])

    def test_range(self):
        self.assertEqual(pypers.task.decode_file_ids('1-2'), [1, 2])
        self.assertEqual(pypers.task.decode_file_ids('1 - 3'), [1, 2, 3])

    def test_invalid(self):
        with self.assertRaises(ValueError):
            pypers.task.decode_file_ids('1-')
        with self.assertRaises(ValueError):
            pypers.task.decode_file_ids('-1')
        with self.assertRaises(ValueError):
            pypers.task.decode_file_ids('-')
        with self.assertRaises(ValueError):
            pypers.task.decode_file_ids('3-1')
        with self.assertRaises(ValueError):
            pypers.task.decode_file_ids('1-1')

    def test_mixed(self):
        self.assertEqual(pypers.task.decode_file_ids('1,2-3,4'), [1, 2, 3, 4])
        self.assertEqual(pypers.task.decode_file_ids('1, 2-3'), [1, 2, 3])


class Task__init(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_with_path(self, path):
        task = pypers.task.Task(
            path = pathlib.Path(path),
            parent = None,
            spec = dict(),
        )
        self.assertEqual(task.path, pathlib.Path(path))

    @testsuite.with_temporary_paths(1)
    def test_with_str_path(self, path):
        task = pypers.task.Task(
            path = str(path),
            parent = None,
            spec = dict(),
        )
        self.assertEqual(task.path, pathlib.Path(path))

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

    @testsuite.with_temporary_paths(3)
    def test_with_parent(self, path1, path2, path3):
        task1 = pypers.task.Task(
            path = path1,
            parent = None,
            spec = dict(
                field1 = 1,
                field2 = 2,
            ),
        )
        task2 = pypers.task.Task(
            path = path2,
            parent = task1,
            spec = dict(
                field2 = 3,
                field3 = 4,
            ),
        )
        task3 = pypers.task.Task(
            path = path3,
            parent = task2,
            spec = dict(
                field2 = 5,
            ),
        )
        self.assertEqual(
            task2.full_spec,
            dict(
                field1 = 1,
                field2 = 3,
                field3 = 4,
            ),
        )
        self.assertEqual(
            task3.full_spec,
            dict(
                field1 = 1,
                field2 = 5,
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

    stage1_cls = testsuite.create_stage_class(id = 'stage1')
    stage2_cls = testsuite.create_stage_class(id = 'stage2')

    @testsuite.with_temporary_paths(1)
    def test_from_spec_with_stages(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                pipeline = [
                    'tests.test_task.Task__create_pipeline.stage1_cls',
                    'tests.test_task.Task__create_pipeline.stage2_cls',
                ],
            ),
        )
        self.assertIsInstance(task.create_pipeline(), pypers.pipeline.Pipeline)
        self.assertEqual(frozenset([stage.id for stage in task.create_pipeline().stages]), {'stage1', 'stage2'})


class Task__get_path_pattern(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_defined(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(cfg_pathpattern = 'cfg/%s.yml'),
        )
        self.assertEqual(task.get_path_pattern('cfg_pathpattern'), path / 'cfg/%s.yml')

    @testsuite.with_temporary_paths(1)
    def test_undefined(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertIsNone(task.get_path_pattern('cfg_pathpattern'))

    @testsuite.with_temporary_paths(1)
    def test_undefined_with_default(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertEqual(task.get_path_pattern('cfg_pathpattern', default = 'cfg/%s.yml'), path / 'cfg/%s.yml')


class Task__create_config(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        # Verify that changes to the config do not affect the spec or full spec (if a config is defined)
        task = pypers.task.Task(
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
        task1 = pypers.task.Task(
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
        task2 = pypers.task.Task(
            path = task2_path,
            parent = task1,
            spec = dict(
                base_config_path = base_config_path,
                config = dict(key2 = 'value200'),
            ),
        )
        task3 = pypers.task.Task(
            path = task3_path,
            parent = task2,
            spec = dict(
                config = dict(key4 = 'value4'),
            ),
        )
        config = task3.create_config()
        self.assertEqual(config, pypers.config.Config(dict(key1 = 'value10', key2 = 'value200', key3 = 'value3', key4 = 'value4')))


class Task__file_ids(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_str(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                file_ids = '1, 2-3, 4',
            ),
        )
        self.assertEqual(task.file_ids, [1, 2, 3, 4])

    @testsuite.with_temporary_paths(1)
    def test_list(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                file_ids = [
                    'id-1',
                    'id-2',
                ],
            ),
        )
        self.assertEqual(task.file_ids, ['id-1', 'id-2'])


class Task__root(unittest.TestCase):

    @testsuite.with_temporary_paths(3)
    def test(self, path1, path2, path3):
        task1 = pypers.task.Task(
            path = path1,
            parent = None,
            spec = dict(),
        )
        task2 = pypers.task.Task(
            path = path2,
            parent = task1,
            spec = dict(),
        )
        task3 = pypers.task.Task(
            path = path3,
            parent = task2,
            spec = dict(),
        )
        self.assertIs(task1.root, task1)
        self.assertIs(task2.root, task1)
        self.assertIs(task3.root, task1)


class Task__resolve_path(unittest.TestCase):

    @testsuite.with_temporary_paths(2)
    def test(self, path1, path2):
        task1 = pypers.task.Task(
            path = path1,
            parent = None,
            spec = dict(),
        )
        task2 = pypers.task.Task(
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


class Task__pending(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_not_runnable(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        config = task.create_config()
        self.assertFalse(task.pending(config))

    @testsuite.with_temporary_paths(1)
    def test_without_digest(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        config = task.create_config()
        self.assertTrue(task.pending(config))

    @testsuite.with_temporary_paths(1)
    def test_with_digest(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        config = task.create_config()
        task.resolve_path('.digest.sha').write_text(config.sha.hexdigest())
        self.assertFalse(task.pending(config))

    @testsuite.with_temporary_paths(1)
    def test_with_wrong_digest(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(runnable = True),
        )
        config = task.create_config()
        task.resolve_path('.digest.sha').write_text(config.sha.hexdigest())
        config['key'] = 'value'
        self.assertTrue(task.pending(config))


class Task__marginal_states(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_from_spec_missing(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertEqual(task.marginal_stages, [])

    @testsuite.with_temporary_paths(1)
    def test_from_spec(self, path):
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                marginal_stages = [
                    'stage1',
                    'stage2',
                ],
            ),
        )
        self.assertEqual(task.marginal_stages, ['stage1', 'stage2'])

    @testsuite.with_temporary_paths(1)
    def test_override(self, path):
        class DerivedTask(pypers.task.Task):

            marginal_stages = [
                'stage1',
                'stage2',
            ]

        task = DerivedTask(
            path = path,
            parent = None,
            spec = dict(),
        )
        self.assertEqual(task.marginal_stages, ['stage1', 'stage2'])


class Task__get_marginal_fields(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test_empty_pipeline(self, path):
        task = pypers.task.Task(
            path = '',
            parent = None,
            spec = dict(pipeline = 'pypers.pipeline.Pipeline'),
        )
        self.assertEqual(task.get_marginal_fields(task.create_pipeline()), frozenset())

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        class DerivedTask(pypers.task.Task):

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
                return pypers.pipeline.create_pipeline(stages, *args, **kwargs)
        
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
        task = pypers.task.Task(
            path = path,
            parent = None,
            spec = dict(
                runnable = True,
                file_ids = ['file-0'],
                marginal_stages = [
                    'stage2',
                ],
                config = dict(
                    key1 = 'value1',
                ),
            ),
        )
        pipeline = pypers.pipeline.create_pipeline(
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
        task.store(pipeline, data, config)

        # Load the stored data
        with gzip.open(task.data_filepath, 'rb') as data_file:
            stored_data = dill.load(data_file)

        # Load the stored digest config
        with task.digest_json_filepath.open('r') as digest_json_file:
            stored_config = pypers.config.Config(json.load(digest_json_file))

        self.assertFalse(task.pending(config))
        self.assertEqual(
            stored_data,
            {
                'file-0': {
                    'output1.1': 'value1.1',
                    'output3.1': 'value3.1',
                }
            },
        )
        self.assertEqual(stored_config, config)


class Task__pickup(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.task = pypers.task.Task(
            path = pathlib.Path(self.tempdir.name),
            parent = None,
            spec = dict(
                runnable = True,
                file_ids = ['file-0'],
                marginal_stages = [
                    'stage2',
                ],
            ),
        )
        self.data_without_marginals = {
            'file-0': {
                'output1.1': 'value1.1',
                'output3.1': 'value3.1',
            },
        }
        with gzip.open(self.task.data_filepath, 'wb') as data_file:
            dill.dump(self.data_without_marginals, data_file, byref=True)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_without_pipeline(self):
        data = self.task.pickup()
        self.assertEqual(data, self.data_without_marginals)

    @patch.object(pypers.pipeline.Pipeline, 'fields', {'output1.1', 'output2.1', 'output2.2', 'output3.1'})  # FIXME: Remove this hack when the pipeline is migrated to use `file_id` instead of `input`
    def test_with_pipeline(self):
        pipeline = pypers.pipeline.create_pipeline(
            [
                testsuite.create_stage(id = 'stage1', outputs = ['output1.1']),
                testsuite.create_stage(id = 'stage2', inputs = ['output1.1'], outputs = ['output2.1', 'output2.2']),
                testsuite.create_stage(id = 'stage3', inputs = ['output1.1', 'output2.1', 'output2.2'], outputs = ['output3.1']),
            ]
        )
        data = self.task.pickup(pipeline)
        self.assertEqual(data, self.data_without_marginals)


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