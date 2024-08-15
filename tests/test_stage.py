import unittest
from unittest.mock import MagicMock

import pypers.stage
import pypers.config

from . import testsuite


class suggest_id(unittest.TestCase):

    def test(self):
        self.assertEqual(pypers.stage.suggest_stage_id('TheGreatPCMapper'     ), 'the-great-pc-mapper'    )
        self.assertEqual(pypers.stage.suggest_stage_id('TheGreat_PCMapper'    ), 'the-great-pc-mapper'    )
        self.assertEqual(pypers.stage.suggest_stage_id('TheGreat__PCMapper'   ), 'the-great-pc-mapper'    )
        self.assertEqual(pypers.stage.suggest_stage_id('TheGreat_123_PCMapper'), 'the-great-123-pc-mapper')
        self.assertEqual(pypers.stage.suggest_stage_id('TheGreat123_PCMapper' ), 'the-great-123-pc-mapper')
        self.assertEqual(pypers.stage.suggest_stage_id('TheGreat123PCMapper'  ), 'the-great-123-pc-mapper')
        self.assertEqual(pypers.stage.suggest_stage_id('TheGreatMapperStage'  ), 'the-great-mapper'       )
        self.assertEqual(pypers.stage.suggest_stage_id('Stage'                ), 'stage'                  )

    def test_illegal(self):
        self.assertRaises(AssertionError, lambda: pypers.stage.suggest_stage_id(''))
        self.assertRaises(AssertionError, lambda: pypers.stage.suggest_stage_id('_'))
        self.assertRaises(AssertionError, lambda: pypers.stage.suggest_stage_id('_1'))
        self.assertRaises(AssertionError, lambda: pypers.stage.suggest_stage_id('TheGreat PCMapper'))
        self.assertRaises(AssertionError, lambda: pypers.stage.suggest_stage_id('TheGreat-PCMapper'))
        self.assertRaises(AssertionError, lambda: pypers.stage.suggest_stage_id('1TheGreatPCMapper'))


class Stage(unittest.TestCase):

    def test_no_inputs_no_outputs(self):
        stage = testsuite.create_stage(id = 'test')
        data  = dict()
        cfg   = pypers.config.Config()
        dt    = stage(data, cfg)
        self.assertIsInstance(dt, float)
        self.assertEqual(data, dict())

    def test_init(self):
        class Stage(pypers.stage.Stage):
            pass
        self.assertEqual(Stage().id, 'stage')

    def test(self):
        stage = testsuite.create_stage(id = 'test', inputs = ['x1', 'x2'], outputs = ['y'], \
            process = lambda x1, x2, config, log_root_dir = None, status = None: \
                dict(y = \
                    x1 * config.get('x1_factor', 0) + \
                    x2 * config.get('x2_factor', 0))
            )
        cfg = pypers.config.Config()
        for x1_factor in [0, 1]:
            for x2_factor in [0, 1]:
                x1, x2 = 10, 20
                with self.subTest(x1_factor = x1_factor, x2_factor = x2_factor):
                    cfg['test/x1_factor'] = x1_factor
                    cfg['test/x2_factor'] = x2_factor
                    data = dict(x1 = x1, x2 = x2)
                    status_mock = MagicMock()
                    dt = stage(data, cfg, status = status_mock)
                    self.assertEqual(data, dict(x1 = x1, x2 = x2, y = x1 * x1_factor + x2 * x2_factor))
                    self.assertIsInstance(dt, float)

    def test_missing_input(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda x, config, log_root_dir = None, status = None: \
                dict(y = x)
            )
        data = dict(x = 0)
        config = pypers.config.Config()
        self.assertRaises(TypeError, lambda: stage(data, config))

    def test_missing_output(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda config, log_root_dir = None, status = None: \
                dict()
            )
        data = dict()
        config = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, config))

    def test_spurious_output(self):
        stage = testsuite.create_stage( id = 'test', \
            process = lambda config, log_root_dir = None, status = None: \
                dict(y = 0)
            )
        data = dict()
        config = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, config))

    def test_missing_and_spurious_output(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda config, log_root_dir = None, status = None: \
                dict(z = 0)
            )
        data = dict()
        config = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, config))

    def test_consumes(self):
        stage = testsuite.create_stage(id = 'test', consumes = ['x'], \
            process = lambda x, config, log_root_dir = None, status = None: \
                dict()
            )
        data = dict(x = 0, y = 1)
        config = pypers.config.Config()
        stage(data, config)
        self.assertEqual(data, dict(y = 1))

    def test_missing_consumes(self):
        stage = testsuite.create_stage(id = 'test', consumes = ['x'], \
            process = lambda x, config, log_root_dir = None, status = None: \
                dict()
            )
        data = dict()
        config = pypers.config.Config()
        self.assertRaises(KeyError, lambda: stage(data, config))