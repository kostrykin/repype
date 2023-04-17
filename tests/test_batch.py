import unittest
import pathlib
import re

import pypers.pipeline
import pypers.batch
import pypers.config

from . import testsuite


rootdir = pathlib.Path(__file__).parent / 'batch'


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
    
    def test_get_task(self):
        batch = self.test_load()
        for expected_task in batch.tasks:
            actual_task = batch.get_task(expected_task.path)
            self.assertIs(actual_task, expected_task)
        self.assertIsNone(batch.get_task(''))


class Task(unittest.TestCase):

    def setUp(self):
        self.batch = pypers.batch.BatchLoader(pypers.batch.Task)
        self.batch.load(rootdir)

    def test_config(self):
        expected_task1_config = pypers.config.Config({
            'stage1': dict(x1 = 0),
            'stage2': dict(x2 = 0),
            'stage3': dict(x3 = 0),
        })
        expected_task2_config = expected_task1_config
        expected_task3_config = pypers.config.Config({
            'stage1': dict(x1 = 0),
        })
        self.assertEqual(self.batch.get_task(rootdir / 'task1').config, expected_task1_config)
        self.assertEqual(self.batch.get_task(rootdir / 'task1' / 'x1=1').config, expected_task1_config.derive({'stage1': dict(x1=1)}))
        self.assertEqual(self.batch.get_task(rootdir / 'task1' / 'x2=1').config, expected_task1_config.derive({'stage2': dict(x2=1)}))
        self.assertEqual(self.batch.get_task(rootdir / 'task1' / 'x3=1').config, expected_task1_config.derive({'stage3': dict(x3=1)}))
        self.assertEqual(self.batch.get_task(rootdir / 'task1' / 'x3=1' / 'x2=1').config, expected_task1_config.derive({'stage3': dict(x3=1), 'stage2': dict(x2=1)}))
        self.assertEqual(self.batch.get_task(rootdir / 'task1' / 'x2=1' / 'x2=0').config, expected_task1_config)
        self.assertEqual(self.batch.get_task(rootdir / 'task2' / 'x2=1').config, expected_task2_config.derive({'stage2': dict(x2=1)}))
        self.assertEqual(self.batch.get_task(rootdir / 'task2' / 'x2=1' / 'x2=0').config, expected_task2_config)
        self.assertEqual(self.batch.get_task(rootdir / 'task3').config, expected_task3_config)
        self.assertEqual(self.batch.get_task(rootdir / 'task3' / 'runnable=false' / 'runnable=true').config, expected_task3_config)

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
            ],
            [
                rootdir / 'task1' / 'x2=1',
                rootdir / 'task2' / 'x2=1',
            ],
            [
                rootdir / 'task3',
                rootdir / 'task3' / 'runnable=false' / 'runnable=true',
            ]
        ]
        classes += [[task.path] for task in self.batch.tasks if task.runnable and all([task.path not in cls for cls in classes])]
        for cls_idx, cls in enumerate(classes):
            task1 = self.batch.get_task(cls[0])
            digest1 = task1.config_digest
            for task2 in (self.batch.get_task(path) for path in cls[1:]):
                digest2 = task2.config_digest
                self.assertEqual(digest1, digest2)
            for cls2_idx, cls2 in enumerate(classes):
                if cls2_idx == cls_idx: continue
                task2 = self.batch.get_task(cls2[0])
                digest2 = task2.config_digest
                self.assertNotEqual(digest1, digest2, f'{task1}, {task2}')

    def test_pickup_previous_task(self):
        pass


if __name__ == '__main__':
    unittest.main()
