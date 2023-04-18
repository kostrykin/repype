import unittest
import pathlib
import re
import json
import gzip
import dill

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


class BatchLoader(unittest.TestCase):

    def test_load(self):
        batch = pypers.batch.BatchLoader(pypers.batch.Task)
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
        def get_number_from_json_file(filepath):
            with open(filepath) as fin:
                return float(json.load(fin))
        stages = [
            ## stage1 takes `input` and produces `a`
            testsuite.DummyStage('stage1', ['input'], ['a'], [], \
                lambda input_data, cfg, log_root_dir = None, out = None: \
                    dict(a = get_number_from_json_file(input_data['input']) * cfg['x1'])
            ),
            ## stage2 takes `a` and produces `b`
            testsuite.DummyStage('stage2', ['a'], ['b'], [], \
                lambda input_data, cfg, log_root_dir = None, out = None: \
                    dict(b = input_data['a'] + cfg['x2'])
            ),
            ## stage3 takes `b` and produces `c`
            testsuite.DummyStage('stage3', ['b'], ['c'], [], \
                lambda input_data, cfg, log_root_dir = None, out = None: \
                    dict(c = input_data['b'] * cfg['x3'])
            ),
        ]
        return pypers.pipeline.create_pipeline(stages)


class Task(unittest.TestCase):

    def setUp(self):
        self.batch = pypers.batch.BatchLoader(DummyTask)
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


if __name__ == '__main__':
    unittest.main()
