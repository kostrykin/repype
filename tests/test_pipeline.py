import itertools
import unittest

import pypers.pipeline
import pypers.config

from . import testsuite


class Stage(unittest.TestCase):

    def test_no_inputs_no_outputs(self):
        stage = testsuite.DummyStage('test', [], [], [], None)
        data  = dict()
        cfg   = pypers.config.Config()
        dt    = stage(data, cfg, out = 'muted')
        self.assertIsInstance(dt, float)
        self.assertEqual(data, dict())

    def test(self):
        stage = testsuite.DummyStage('test', ['x1', 'x2'], ['y'], [], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict(y = \
                    input_data['x1'] * cfg.get('x1_factor', 0) + \
                    input_data['x2'] * cfg.get('x2_factor', 0))
            )
        cfg = pypers.config.Config()
        for x1_factor in [0, 1]:
            for x2_factor in [0, 1]:
                x1, x2 = 10, 20
                with self.subTest(x1_factor = x1_factor, x2_factor = x2_factor):
                    cfg['test/x1_factor'] = x1_factor
                    cfg['test/x2_factor'] = x2_factor
                    data = dict(x1 = x1, x2 = x2)
                    dt   = stage(data, cfg, out = 'muted')
                    self.assertEqual(data, dict(x1 = x1, x2 = x2, y = x1 * x1_factor + x2 * x2_factor))
                    self.assertIsInstance(dt, float)

    def test_missing_input(self):
        stage = testsuite.DummyStage('test', [], ['y'], [], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict(y = input_data['x'])
           )
        data = dict(x = 0)
        cfg  = pypers.config.Config()
        self.assertRaises(KeyError, lambda: stage(data, cfg, out = 'muted'))

    def test_missing_output(self):
        stage = testsuite.DummyStage('test', [], ['y'], [], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict()
           )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))

    def test_spurious_output(self):
        stage = testsuite.DummyStage('test', [], [], [], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict(y = 0)
           )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))

    def test_missing_and_spurious_output(self):
        stage = testsuite.DummyStage('test', [], ['y'], [], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict(z = 0)
           )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))

    def test_consumes(self):
        stage = testsuite.DummyStage('test', [], [], ['x'], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict()
           )
        data = dict(x = 0, y = 1)
        cfg  = pypers.config.Config()
        stage(data, cfg, out = 'muted')
        self.assertEqual(data, dict(y = 1))

    def test_missing_consumes(self):
        stage = testsuite.DummyStage('test', [], [], ['x'], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict()
           )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(KeyError, lambda: stage(data, cfg, out = 'muted'))


class Pipeline(unittest.TestCase):

    def _test_Pipeline_configure(self, original_base_cfg: 'config.Config'):
        factors = list(range(-1, +3))
        for x1_pre_factor, x2_pre_factor in itertools.product(factors, factors):
            for x1_default_user_factor, x2_default_user_factor in itertools.product(factors, factors):
                with self.subTest(x1_pre_factor = x1_pre_factor, x2_pre_factor = x2_pre_factor, x1_default_user_factor = x1_default_user_factor, x2_default_user_factor = x2_default_user_factor):
                    base_cfg = original_base_cfg.copy()
                    cfg = create_pipeline().test(
                            lambda x1_factor_specs, x2_factor_specs: dict(x1_factor = x1_factor_specs),
                            lambda x1_factor_specs, x2_factor_specs: dict(x2_factor = x2_factor_specs),
                        ).configure(base_cfg,
                            x1_factor_specs = (x1_pre_factor, x1_default_user_factor),
                            x2_factor_specs = (x2_pre_factor, x2_default_user_factor),
                        )
                    self.assertEqual(base_cfg, original_base_cfg)
                    self.assertEqual(cfg['stage1/x1_factor'], base_cfg.get('stage1/x1_factor', x1_pre_factor * base_cfg.get('stage1/AF_x1_factor', x1_default_user_factor)))
                    self.assertEqual(cfg['stage2/x2_factor'], base_cfg.get('stage2/x2_factor', x2_pre_factor * base_cfg.get('stage2/AF_x2_factor', x2_default_user_factor)))

    def test_Pipeline_configure(self):
        cfg = pypers.config.Config()
        self._test_Pipeline_configure(cfg)

        cfg = pypers.config.Config()
        cfg['stage1/x1_factor'] = 1
        self._test_Pipeline_configure(cfg)

        cfg = pypers.config.Config()
        cfg['stage1/AF_x1_factor'] = 1
        self._test_Pipeline_configure(cfg)

    def test_Pipeline_find(self):
        pipeline = create_pipeline().test()
        for expected_position, stage in enumerate(pipeline.stages):
            with self.subTest(expected_position = expected_position):
                actual_position = pipeline.find(stage.cfgns)
                self.assertEqual(actual_position, expected_position)

    def test_Pipeline_find_missing(self):
        pipeline = create_pipeline().test()
        dummy = object()
        self.assertIs(pipeline.find('stage4', dummy), dummy)

    def test_Pipeline_append(self):
        new_stage = testsuite.DummyStage('new_stage', [], [], [], None)
        pipeline = create_pipeline().test()
        expected_pos = len(pipeline.stages)
        pos = pipeline.append(new_stage)
        self.assertEqual(pos, expected_pos)
        self.assertEqual(len(pipeline.stages), expected_pos + 1)
        self.assertIs(pipeline.stages[pos], new_stage)

    def test_Pipeline_append_twice(self):
        pipeline = create_pipeline().test()
        for stage in pipeline.stages:
            with self.subTest(stage = stage.cfgns):
                self.assertRaises(RuntimeError, lambda: pipeline.append(stage))
                self.assertRaises(RuntimeError, lambda: pipeline.append(testsuite.DummyStage(stage.cfgns, [], [], [], None)))

    def test_Pipeline_append_after_str(self):
        new_stage = testsuite.DummyStage('new_stage', [], [], [], None)
        stages = create_pipeline().test().stages
        for after in [stage.cfgns for stage in stages]:
            pipeline = create_pipeline().test()
            expected_pos = pipeline.find(after) + 1
            with self.subTest(after = after):
                pos = pipeline.append(new_stage, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], new_stage)

    def test_Pipeline_append_after_int(self):
        new_stage = testsuite.DummyStage('new_stage', [], [], [], None)
        stages = create_pipeline().test().stages
        for after in range(-1, len(stages)):
            pipeline = create_pipeline().test()
            expected_pos = after + 1
            with self.subTest(after = after):
                pos = pipeline.append(new_stage, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], new_stage)

    def test_Pipeline_process(self):
        x1_factor = 1
        x2_factor = 2
        constant  = 3
        cfg = pypers.config.Config()
        cfg['stage1/x1_factor'] = x1_factor
        cfg['stage2/x2_factor'] = x2_factor
        cfg['stage3/constant' ] = constant
        pipeline = create_pipeline().test()
        for input in range(5):
            with self.subTest(input = input):
                data, final_cfg, timings = pipeline.process(input = input, cfg = cfg, out = 'muted')
                expected_final_cfg = cfg.copy()
                for stage in pipeline.stages:
                    expected_final_cfg[f'{stage.cfgns}/enabled'] = True
                    self.assertTrue(stage.cfgns in timings)
                self.assertEqual(final_cfg, expected_final_cfg, f'expected: {expected_final_cfg}, actual: {final_cfg}')
                self.assertEqual(data['y'], x1_factor * input + x2_factor * input + constant)
                self.assertEqual(len(timings), len(pipeline.stages))


class create_pipeline(unittest.TestCase):

    def test(self, configure_stage1 = None, configure_stage2 = None, configure_stage3 = None):
        stages = [
            ## stage1 takes `input` and produces `x1`
            testsuite.DummyStage('stage1', ['input'], ['x1'], [], \
                lambda input_data, cfg, log_root_dir = None, out = None: \
                    dict(x1 = input_data['input'] * cfg['x1_factor']), \
                configure_stage1
            ),
            ## stage2 consumes `input` and produces `x2`
            testsuite.DummyStage('stage2', [], ['x2'], ['input'], \
                lambda input_data, cfg, log_root_dir = None, out = None: \
                    dict(x2 = input_data['input'] * cfg['x2_factor']),
                configure_stage2
            ),
            ## stage3 takes `x1` and `x2` and produces `y`
            testsuite.DummyStage('stage3', ['x1', 'x2'], ['y'], [], \
                lambda input_data, cfg, log_root_dir = None, out = None: \
                    dict(y = input_data['x1'] + input_data['x2'] + cfg['constant']),
                configure_stage3
            ),
        ]
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                pipeline = pypers.pipeline.create_pipeline(permutated_stages)
                self.assertEqual(len(pipeline.stages), 3)
                for stage1, stage2 in zip(pipeline.stages, stages):
                    self.assertIs(stage1, stage2)
        return pipeline

    def test_unsatisfiable(self):
        stages = [
            testsuite.DummyStage('stage1', ['input'], ['x1'], [], None),     ## stage1 takes `input` and produces `x1`
            testsuite.DummyStage('stage2', [], ['x2'], ['input'], None),     ## stage2 consumes `input` and produces `x2`
            testsuite.DummyStage('stage3', ['x1', 'x2'], ['y'], [], None),   ## stage3 takes `x1` and `x2` and produces `y`
            testsuite.DummyStage('stage4', ['input', 'y'], ['z'], [], None), ## stage4 takes `input` and `y` and produces `z`
        ]
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                self.assertRaises(RuntimeError, lambda: pypers.pipeline.create_pipeline(permutated_stages))

    def test_ambiguous_namespaces(self):
        stage1 = testsuite.DummyStage('stage1', [], [], [], None)
        stage2 = testsuite.DummyStage('stage2', [], [], [], None)
        stage3 = testsuite.DummyStage('stage2', [], [], [], None)
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage3]))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage2]))

    def test_ambiguous_outputs(self):
        stage1 = testsuite.DummyStage('stage1', [], ['x1'], [], None)
        stage2 = testsuite.DummyStage('stage2', [], ['x2'], [], None)
        stage3 = testsuite.DummyStage('stage3', [], ['x2'], [], None)
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage3]))


if __name__ == '__main__':
    unittest.main()
