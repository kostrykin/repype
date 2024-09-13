import json
import multiprocessing
import unittest
from unittest.mock import (
    MagicMock,
    call,
)

import dill

import repype.config
import repype.stage

from . import testsuite


class suggest_id(unittest.TestCase):

    def test(self):
        self.assertEqual(repype.stage.suggest_stage_id('TheGreatPCMapper'     ), 'the-great-pc-mapper'    )
        self.assertEqual(repype.stage.suggest_stage_id('TheGreat_PCMapper'    ), 'the-great-pc-mapper'    )
        self.assertEqual(repype.stage.suggest_stage_id('TheGreat__PCMapper'   ), 'the-great-pc-mapper'    )
        self.assertEqual(repype.stage.suggest_stage_id('TheGreat_123_PCMapper'), 'the-great-123-pc-mapper')
        self.assertEqual(repype.stage.suggest_stage_id('TheGreat123_PCMapper' ), 'the-great-123-pc-mapper')
        self.assertEqual(repype.stage.suggest_stage_id('TheGreat123PCMapper'  ), 'the-great-123-pc-mapper')
        self.assertEqual(repype.stage.suggest_stage_id('TheGreatMapperStage'  ), 'the-great-mapper'       )
        self.assertEqual(repype.stage.suggest_stage_id('Stage'                ), 'stage'                  )
        self.assertEqual(repype.stage.suggest_stage_id('stage1_abc_cls'       ), 'stage-1-abc-cls'        )

    def test_illegal(self):
        self.assertRaises(AssertionError, lambda: repype.stage.suggest_stage_id(''))
        self.assertRaises(AssertionError, lambda: repype.stage.suggest_stage_id('_'))
        self.assertRaises(AssertionError, lambda: repype.stage.suggest_stage_id('_1'))
        self.assertRaises(AssertionError, lambda: repype.stage.suggest_stage_id('TheGreat PCMapper'))
        self.assertRaises(AssertionError, lambda: repype.stage.suggest_stage_id('TheGreat-PCMapper'))
        self.assertRaises(AssertionError, lambda: repype.stage.suggest_stage_id('1TheGreatPCMapper'))


class Stage(unittest.TestCase):

    def test_no_inputs_no_outputs(self):
        stage    = testsuite.create_stage(id = 'test')
        data     = dict()
        config   = repype.config.Config()
        pipeline = MagicMock()
        dt = stage.run(pipeline, data, config)
        self.assertIsInstance(dt, float)
        self.assertEqual(data, dict())

    def test_init(self):
        class Stage(repype.stage.Stage):
            pass
        self.assertEqual(Stage().id, 'stage')

    def test(self):
        stage = testsuite.create_stage(id = 'test', inputs = ['x1', 'x2'], outputs = ['y'], \
            process = lambda pipeline, x1, x2, config, status = None: \
                dict(y = \
                    x1 * config.get('x1_factor', 0) + \
                    x2 * config.get('x2_factor', 0))
            )
        config = repype.config.Config()
        for x1_factor in [0, 1]:
            for x2_factor in [0, 1]:
                x1, x2 = 10, 20
                with self.subTest(x1_factor = x1_factor, x2_factor = x2_factor):
                    config['x1_factor'] = x1_factor
                    config['x2_factor'] = x2_factor
                    data = dict(x1 = x1, x2 = x2)
                    status_mock = MagicMock()
                    pipeline = MagicMock()
                    dt = stage.run(pipeline, data, config, status = status_mock)
                    self.assertEqual(data, dict(x1 = x1, x2 = x2, y = x1 * x1_factor + x2 * x2_factor))
                    self.assertIsInstance(dt, float)

    def test_missing_input(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda pipeline, x, config, status = None: \
                dict(y = x)
            )
        data = dict(x = 0)
        config = repype.config.Config()
        pipeline = MagicMock()
        with self.assertRaises(TypeError):
            stage.run(pipeline, data, config)

    def test_missing_output(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda pipeline, config, status = None: \
                dict()
            )
        data = dict()
        config = repype.config.Config()
        pipeline = MagicMock()
        with self.assertRaises(AssertionError):
            stage.run(pipeline, data, config)

    def test_spurious_output(self):
        stage = testsuite.create_stage( id = 'test', \
            process = lambda pipeline, config, status = None: \
                dict(y = 0)
            )
        data = dict()
        config = repype.config.Config()
        pipeline = MagicMock()
        with self.assertRaises(AssertionError):
            stage.run(pipeline, data, config)

    def test_missing_and_spurious_output(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda pipeline, config, status = None: \
                dict(z = 0)
            )
        data = dict()
        config = repype.config.Config()
        pipeline = MagicMock()
        with self.assertRaises(AssertionError):
            stage.run(pipeline, data, config)

    def test_consumes(self):
        stage = testsuite.create_stage(id = 'test', consumes = ['x'], \
            process = lambda pipeline, x, config, status = None: \
                dict()
            )
        data = dict(x = 0, y = 1)
        config = repype.config.Config()
        pipeline = MagicMock()
        stage.run(pipeline, data, config)
        self.assertEqual(data, dict(y = 1))

    def test_missing_consumes(self):
        stage = testsuite.create_stage(id = 'test', consumes = ['x'], \
            process = lambda pipeline, x, config, status = None: \
                dict()
            )
        data = dict()
        config = repype.config.Config()
        pipeline = MagicMock()
        with self.assertRaises(KeyError):
            stage.run(pipeline, data, config)


class Stage__callback(unittest.TestCase):

    def setUp(self):
        self.pipeline = MagicMock()
        self.stage = testsuite.create_stage(id = 'test')
        self.callback = MagicMock()
        self.stage.add_callback('start', self.callback)
        self.stage.add_callback('end'  , self.callback)
        self.stage.add_callback('skip' , self.callback)
        self.data = dict(key = 'data')
        self.config = repype.config.Config(
            dict(key = 'config'),
        )

    def test(self):
        self.stage.run(pipeline = self.pipeline, data = self.data, config = self.config)
        self.assertEqual(
            self.callback.call_args_list,
            [
                call(stage = self.stage, event = 'start', pipeline = self.pipeline, data = self.data, status = None, config = self.config),
                call(stage = self.stage, event = 'end', pipeline = self.pipeline, data = self.data, status = None, config = self.config),
            ],
        )

    def test_skip(self):
        self.stage.skip(pipeline = self.pipeline, data = self.data, config = self.config)
        self.assertEqual(
            self.callback.call_args_list,
            [
                call(stage = self.stage, event = 'skip', pipeline = self.pipeline, data = self.data, status = None, config = self.config),
            ],
        )

    def test_skip_disabled(self):
        self.config['enabled'] = False
        self.stage.run(pipeline = self.pipeline, data = self.data, config = self.config)
        self.assertEqual(
            self.callback.call_args_list,
            [
                call(stage = self.stage, event = 'skip', pipeline = self.pipeline, data = self.data, status = None, config = self.config),
            ],
        )


def write_signature(payload, filepath):
    stage = dill.loads(payload)
    with open(filepath, 'w') as file:
        json.dump(stage.signature, file)


class Stage__signature(unittest.TestCase):

    def setUp(self):
        self.stage = testsuite.create_stage(id = 'test')
        self.signature = self.stage.signature

    def test_serialization(self):
        stage_serialized = dill.dumps(self.stage)
        stage = dill.loads(stage_serialized)
        self.assertEqual(self.signature, stage.signature)

    @testsuite.with_temporary_paths(1)
    def test_interprocess(self, path):
        filepath = path / 'signature.json'
        p = multiprocessing.Process(target = write_signature, args = (dill.dumps(self.stage), str(filepath)))
        p.start()
        p.join()
        with filepath.open('r') as file:
            signature = json.load(file)
        self.assertEqual(self.signature, signature)


class Stage__sha(unittest.TestCase):

    def setUp(self):
        self.stage = testsuite.create_stage(id = 'test')
        self.sha = self.stage.sha

    def test_serialization(self):
        stage_serialized = dill.dumps(self.stage)
        stage = dill.loads(stage_serialized)
        self.assertEqual(self.sha, stage.sha)


class Stage__run(unittest.TestCase):

    def test(self):

        class Stage(repype.stage.Stage):

            id = 'stage'

            def process(self, pipeline, config, status = None):
                config.get('key', 'value')
                return dict()

        stage = Stage()
        pipeline = MagicMock()
        config = repype.config.Config()
        stage.run(pipeline, dict(), config)
        self.assertEqual(
            config.entries,
            dict(enabled = True, key = 'value'),
        )
