import unittest
import pathlib
import re
import json
import gzip
import dill
import tarfile

import pypers.pipeline
import pypers.batch
import pypers.config

from . import testsuite


rootdir = pathlib.Path(__file__).parent / 'batch'


def _get_written_results_file(task):
    if hasattr(task, 'result_path') and task.result_path.exists():
        return task.result_path
    elif task.parent_task is not None:
        return _get_written_results_file(task.parent_task)
    else:
        return None
    

def get_number_from_json_file(filepath):
    with open(filepath) as fin:
        return float(json.load(fin))


class BatchLoader(unittest.TestCase):

    def test_load(self):
        batch = pypers.batch.BatchLoader(pypers.batch.Task, pypers.batch.JSONLoader())
        batch.load(rootdir)
        expected_task_list = [
            str(rootdir / 'task1'),                     ## 0
            str(rootdir / 'task1' / 'x3=1'),            ## 1
            str(rootdir / 'task1' / 'x3=1' / 'x2=1'),   ## 2
            str(rootdir / 'task1' / 'x2=1'),            ## 3
            str(rootdir / 'task1' / 'x2=1' / 'x2=0'),   ## 4
            str(rootdir / 'task1' / 'x1=1'),            ## 5
            str(rootdir / 'task2'),                     ## 6
            str(rootdir / 'task2' / 'x2=1'),            ## 7
            str(rootdir / 'task2' / 'x2=1' / 'x2=0'),   ## 8
            str(rootdir / 'task3'),
            str(rootdir / 'task3' / 'runnable=false'),
            str(rootdir / 'task3' / 'runnable=false' / 'runnable=true'),
        ]
        self.assertEqual(frozenset(batch.task_list), frozenset(expected_task_list))
        self.assertLess (batch.task_list.index(expected_task_list[0]), batch.task_list.index(expected_task_list[1]))
        self.assertLess (batch.task_list.index(expected_task_list[0]), batch.task_list.index(expected_task_list[3]))
        self.assertLess (batch.task_list.index(expected_task_list[0]), batch.task_list.index(expected_task_list[5]))
        self.assertLess (batch.task_list.index(expected_task_list[1]), batch.task_list.index(expected_task_list[2]))
        self.assertLess (batch.task_list.index(expected_task_list[3]), batch.task_list.index(expected_task_list[4]))
        self.assertLess (batch.task_list.index(expected_task_list[6]), batch.task_list.index(expected_task_list[7]))
        self.assertLess (batch.task_list.index(expected_task_list[7]), batch.task_list.index(expected_task_list[8]))
        return batch
    
    def test_task(self):
        batch = self.test_load()
        for expected_task in batch.tasks:
            actual_task = batch.task(expected_task.path)
            self.assertIs(actual_task, expected_task)
        self.assertIsNone(batch.task(''))


class DummyTask(pypers.batch.Task):

    def create_pipeline(self, dry: bool = False):
        assert isinstance(dry, bool)
        stages = [
            ## stage1 takes `input` and produces `a`
            testsuite.create_stage(id = 'stage1', inputs = ['input'], outputs = ['a'], process = self.stage1_process),
            ## stage2 takes `a` and produces `b`
            testsuite.create_stage(id = 'stage2', inputs = ['a'], outputs = ['b'], process = self.stage2_process),
            ## stage3 takes `b` and produces `c`
            testsuite.create_stage(id = 'stage3', inputs = ['b'], outputs = ['c'], process = self.stage3_process),
        ]
        return pypers.pipeline.create_pipeline(stages)
    
    def stage1_process(self, input, cfg, log_root_dir = None, out = None):
        input_value = get_number_from_json_file(input)
        if log_root_dir is not None:
            with open(log_root_dir + '/stage1.txt', 'w') as file:
                file.write(f"{input_value} * {cfg['x1']}")
        return dict(a = input_value * cfg['x1'])
    
    def stage2_process(self, a, cfg, log_root_dir = None, out = None):
        if log_root_dir is not None:
            with open(log_root_dir + '/stage2.txt', 'w') as file:
                file.write(f"{a} + {cfg['x2']}")
        return dict(b = a + cfg['x2'])
    
    def stage3_process(self, b, cfg, log_root_dir = None, out = None):
        if log_root_dir is not None:
            with open(log_root_dir + '/stage3.txt', 'w') as file:
                file.write(f"{b} * {cfg['x3']}")
        return dict(c = b * cfg['x3'])


class Task(unittest.TestCase):

    def setUp(self):
        self.batch = pypers.batch.BatchLoader(DummyTask, pypers.batch.JSONLoader())
        self.batch.load(rootdir)

    def tearDown(self):
        for task in self.batch.tasks:
            task.reset()

    def test_config(self):
        expected_base_config = pypers.config.Config({
            'stage1': dict(x1 = 0),
            'stage2': dict(x2 = 0),
            'stage3': dict(x3 = 0),
        })
        self.assertEqual(self.batch.task(rootdir / 'task1').config, expected_base_config)
        self.assertEqual(self.batch.task(rootdir / 'task1' / 'x1=1').config, expected_base_config.derive({'stage1': dict(x1=1)}))
        self.assertEqual(self.batch.task(rootdir / 'task1' / 'x2=1').config, expected_base_config.derive({'stage2': dict(x2=1)}))
        self.assertEqual(self.batch.task(rootdir / 'task1' / 'x3=1').config, expected_base_config.derive({'stage3': dict(x3=1)}))
        self.assertEqual(self.batch.task(rootdir / 'task1' / 'x3=1' / 'x2=1').config, expected_base_config.derive({'stage3': dict(x3=1), 'stage2': dict(x2=1)}))
        self.assertEqual(self.batch.task(rootdir / 'task1' / 'x2=1' / 'x2=0').config, expected_base_config)
        self.assertEqual(self.batch.task(rootdir / 'task2' / 'x2=1').config, expected_base_config.derive({'stage2': dict(x2=1)}))
        self.assertEqual(self.batch.task(rootdir / 'task2' / 'x2=1' / 'x2=0').config, expected_base_config)
        self.assertEqual(self.batch.task(rootdir / 'task3').config, expected_base_config)
        self.assertEqual(self.batch.task(rootdir / 'task3' / 'runnable=false' / 'runnable=true').config, expected_base_config)

    def test_root_path(self):
        get_expected_root_path = lambda task: rootdir / re.match(r'.*(task[123]).*', str(task.path)).group(1)
        for task in self.batch.tasks:
            self.assertEqual(task.root_path, get_expected_root_path(task))

    def test_config_digest(self):
        classes = [
            [
                rootdir / 'task1',
                rootdir / 'task1' / 'x2=1' / 'x2=0',
                rootdir / 'task2' / 'x2=1' / 'x2=0',
                rootdir / 'task3',
                rootdir / 'task3' / 'runnable=false' / 'runnable=true',
            ],
            [
                rootdir / 'task1' / 'x2=1',
                rootdir / 'task2' / 'x2=1',
            ],
        ]
        classes += [[task.path] for task in self.batch.tasks if task.runnable and all([task.path not in cls for cls in classes])]
        for cls_idx, cls in enumerate(classes):
            task1 = self.batch.task(cls[0])
            digest1 = task1.config_digest
            for task2 in (self.batch.task(path) for path in cls[1:]):
                digest2 = task2.config_digest
                self.assertEqual(digest1, digest2)
            for cls2_idx, cls2 in enumerate(classes):
                if cls2_idx == cls_idx: continue
                task2 = self.batch.task(cls2[0])
                digest2 = task2.config_digest
                self.assertNotEqual(digest1, digest2, f'{task1}, {task2}')

    def test_pickup_previous_task(self):
        data = dict()

        task = self.batch.task(rootdir / 'task1')
        self.assertEqual(task.pickup_previous_task(task.create_pipeline(), out = 'muted'), (None, {}))
        data[task.path] = task.run(out = 'muted')

        task = self.batch.task(rootdir / 'task1' / 'x2=1')
        pickup_stage, pickup_data = task.pickup_previous_task(task.create_pipeline(), out = 'muted')
        self.assertEqual(pickup_stage, 'stage2')
        self.assertEqual(pickup_data, data[rootdir / 'task1'])
        data[task.path] = task.run(out = 'muted')

        task = self.batch.task(rootdir / 'task1' / 'x2=1' / 'x2=0')
        pickup_stage, pickup_data = task.pickup_previous_task(task.create_pipeline(), out = 'muted')
        self.assertEqual(pickup_stage, 'stage2') ## TODO: pickup from stage 3 should be possible and better
        self.assertEqual(pickup_data, data[rootdir / 'task1' / 'x2=1'])

        task = self.batch.task(rootdir / 'task3')
        self.assertEqual(task.pickup_previous_task(task.create_pipeline(), out = 'muted'), (None, {}))
        data[task.path] = task.run(out = 'muted')

        task = self.batch.task(rootdir / 'task3' / 'runnable=false' / 'runnable=true')
        pickup_stage, pickup_data = task.pickup_previous_task(task.create_pipeline(), out = 'muted')
        self.assertEqual(pickup_stage, '')
        self.assertEqual(pickup_data, data[rootdir / 'task3'])

    def test_run(self):
        data_actual = dict()
        for task in self.batch.tasks:
            task.run(out = 'muted')
            if hasattr(task, 'result_path'):
                result_path = _get_written_results_file(task)
                with gzip.open(result_path, 'rb') as fin:
                    data_actual[task.path] = dill.load(fin)
            else:
                data_actual[task.path] = None
        for task in self.batch.tasks:
            task.reset()
        for task in self.batch.tasks:
            with self.subTest(task = task):
                data_expected = task.run(out = 'muted')
                if data_expected is None:
                    self.assertIsNone(data_expected)
                else:
                    self.assertEqual(data_actual[task.path], data_expected)

    def test_pending(self):
        for task in self.batch.tasks:
            if task.runnable:
                self.assertTrue(task.is_pending)
            task.run(out = 'muted')
            self.assertFalse(task.is_pending)

    def test_run_twice(self):
        for task in self.batch.tasks:
            data = task.run(out = 'muted')
            if task.runnable:
                self.assertIsNotNone(data)
            self.assertIsNone(task.run(out = 'muted'))

    def test_run_oneshot(self):
        for task in self.batch.tasks:
            check_pending = task.is_pending
            data1 = task.run(out = 'muted', one_shot=True)
            if check_pending:
                self.assertTrue(task.is_pending)
            if task.runnable:
                self.assertIsNotNone(data1)
                data2 = task.run(out = 'muted')
                self.assertIsNotNone(data2)

    def test_run_force(self):
        for task in self.batch.tasks:
            data1 = task.run(out = 'muted')
            if task.runnable:
                self.assertIsNotNone(data1)
                data2 = task.run(out = 'muted', force=True)
                self.assertIsNotNone(data2)

    def test_cfg_pathpattern(self):
        cfg_pathpattern = 'cfg/cfg-%s'
        batch = pypers.batch.BatchLoader(DummyTask, pypers.batch.JSONLoader(), inject = dict(cfg_pathpattern = cfg_pathpattern))
        batch.load(rootdir)
        task = batch.task(rootdir / 'task1')
        self.assertEqual(task.cfg_pathpattern, task.path / cfg_pathpattern)
        try:
            task.run(out = 'muted')
            for file_id in task.file_ids:
                cfg_filepath = task.path / (cfg_pathpattern % file_id + '.json')
                self.assertTrue(cfg_filepath.exists())
                cfg = task.loader.load(cfg_filepath)
                self.assertEqual(cfg, task.config.entries)
        finally:
            task.reset()
            for file_id in task.file_ids:
                cfg_filepath = task.path / (cfg_pathpattern % file_id + '.json')
                self.assertFalse(cfg_filepath.exists())
        return task

    def test_yaml(self):
        yaml_batch = pypers.batch.BatchLoader(DummyTask, pypers.batch.YAMLLoader(), inject = dict(cfg_pathpattern = 'cfg/cfg-%s'))
        yaml_batch.load(rootdir)
        self.assertEqual(yaml_batch.task_list, [str(rootdir / 'yml_task1')])
        yaml_task = yaml_batch.task(rootdir / 'yml_task1')
        json_task = self.test_cfg_pathpattern()
        self.assertEqual(yaml_task, json_task)
        try:
            data_json = json_task.run(out = 'muted')
            data_yaml = yaml_task.run(out = 'muted')
            self.assertEqual(data_yaml, data_json)
            for file_id in yaml_task.file_ids:
                cfg_filepath = pathlib.Path(pypers.batch.resolve_pathpattern(yaml_task.cfg_pathpattern, file_id) + '.yml')
                self.assertTrue(cfg_filepath.exists())
                cfg = yaml_task.loader.load(cfg_filepath)
                self.assertEqual(cfg, json_task.config.entries)
        finally:
            json_task.reset()
            yaml_task.reset()
            for file_id in yaml_task.file_ids:
                cfg_filepath = pathlib.Path(pypers.batch.resolve_pathpattern(yaml_task.cfg_pathpattern, file_id) + '.yml')
                self.assertFalse(cfg_filepath.exists())

    def test_log_pathpattern(self):
        log_pathpattern = 'log/log-%s'
        batch = pypers.batch.BatchLoader(DummyTask, pypers.batch.JSONLoader(), inject = dict(log_pathpattern = log_pathpattern))
        batch.load(rootdir)
        task = batch.task(rootdir / 'task1')
        self.assertEqual(task.log_pathpattern, task.path / log_pathpattern)
        try:
            task.run(out = 'muted')
            for file_id in task.file_ids:
                log_filepath = task.path / (log_pathpattern % file_id + '.tgz')
                with self.subTest(file_id = file_id):
                    self.assertTrue(log_filepath.exists())
                    with tarfile.open(log_filepath, 'r:gz') as file:
                        self.assertEqual(frozenset(file.getnames()) - {''}, frozenset(['stage1.txt', 'stage2.txt', 'stage3.txt']))
        finally:
            task.reset()
            for file_id in task.file_ids:
                log_filepath = task.path / (log_pathpattern % file_id + '.tgz')
                self.assertFalse(log_filepath.exists())

    def test_get_marginal_fields(self):
        for task in self.batch.tasks:
            pipeline = task.create_pipeline()
            self.assertEqual(task.get_marginal_fields(pipeline), set())

class ExtendedTask(DummyTask):

    outputs = ['result_a', 'result_c']
    
    def is_stage_marginal(self, stage):
        if stage == 'stage1' or stage == 'stage3':
            return True
        else:
            return False

    def create_pipeline(self, *args, **kwargs):
        pipeline = super(ExtendedTask, self).create_pipeline(*args, **kwargs)
        pipeline.stages[0].add_callback('end' , self.write_intermediate_results)
        pipeline.stages[2].add_callback('end' , self.write_intermediate_results)
        return pipeline
        
    def write_intermediate_results(self, stage, cb_name, data, result_a_filepath, result_c_filepath, out):
        if stage.id == 'stage1':
            with open(result_a_filepath, 'w') as fout:
                json.dump(data['a'], fout)
        if stage.id == 'stage3':
            with open(result_c_filepath, 'w') as fout:
                json.dump(data['c'], fout)


class ExtendedTaskTest(unittest.TestCase):

    def setUp(self):
        self.batch = pypers.batch.BatchLoader(ExtendedTask, pypers.batch.JSONLoader())
        self.batch.load(rootdir)

    def tearDown(self):
        for task in self.batch.tasks:
            task.reset()

    def test_get_marginal_fields(self):
        for task in self.batch.tasks:
            pipeline = task.create_pipeline()
            self.assertEqual(task.get_marginal_fields(pipeline), set(['a', 'c']))

    def test_run(self):
        for task in self.batch.tasks:
            if not str(task.path).startswith(str(self.batch.task(rootdir / 'task1'))): continue
            pipeline = task.create_pipeline()
            task.run(out = 'muted')
            if hasattr(task, 'result_path'):
                result_path = _get_written_results_file(task)
                with gzip.open(result_path, 'rb') as fin:
                    data = dill.load(fin)
                for field in task.get_marginal_fields(pipeline):
                    with self.subTest(task = task, field = field):
                        self.assertFalse(field in data.keys())

    def test_pickup_previous_task(self):
        self.batch.task(rootdir / 'task1').run(out = 'muted')
        task = self.batch.task(rootdir / 'task1' / 'x2=1')
        self.assertEqual(task.pickup_previous_task(task.create_pipeline(), out = 'muted'), (None, {}))

    def test_intermediate_results(self):
        self.batch.task(rootdir / 'task1').run(out = 'muted')
        task = self.batch.task(rootdir / 'task1' / 'x2=1')
        expected_data = task.run(out = 'muted')
        for file_id in task.file_ids:
            with self.subTest(file_id = file_id):
                self.assertEqual(get_number_from_json_file(task.path / 'results_a' / f'{file_id}.json'), expected_data[file_id]['a'])
                self.assertEqual(get_number_from_json_file(task.path / 'results_c' / f'{file_id}.json'), expected_data[file_id]['c'])
        return task

    def test_reset(self):
        task = self.test_intermediate_results()
        task.reset()
        for file_id in task.file_ids:
            with self.subTest(file_id = file_id):
                self.assertFalse((task.path / 'results_a' / f'{file_id}.json').exists())
                self.assertFalse((task.path / 'results_c' / f'{file_id}.json').exists())


if __name__ == '__main__':
    unittest.main()
