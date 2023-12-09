import itertools
import unittest

import pypers.pipeline
import pypers.config

from . import testsuite


class suggest_id(unittest.TestCase):

    def test(self):
        self.assertEqual(pypers.pipeline.suggest_stage_id('TheGreatPCMapper'     ), 'the-great-pc-mapper'    )
        self.assertEqual(pypers.pipeline.suggest_stage_id('TheGreat_PCMapper'    ), 'the-great-pc-mapper'    )
        self.assertEqual(pypers.pipeline.suggest_stage_id('TheGreat__PCMapper'   ), 'the-great-pc-mapper'    )
        self.assertEqual(pypers.pipeline.suggest_stage_id('TheGreat_123_PCMapper'), 'the-great-123-pc-mapper')
        self.assertEqual(pypers.pipeline.suggest_stage_id('TheGreat123_PCMapper' ), 'the-great-123-pc-mapper')
        self.assertEqual(pypers.pipeline.suggest_stage_id('TheGreat123PCMapper'  ), 'the-great-123-pc-mapper')
        self.assertEqual(pypers.pipeline.suggest_stage_id('TheGreatMapperStage'  ), 'the-great-mapper'       )
        self.assertEqual(pypers.pipeline.suggest_stage_id('Stage'                ), 'stage'                  )

    def test_illegal(self):
        self.assertRaises(AssertionError, lambda: pypers.pipeline.suggest_stage_id(''))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.suggest_stage_id('_'))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.suggest_stage_id('_1'))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.suggest_stage_id('TheGreat PCMapper'))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.suggest_stage_id('TheGreat-PCMapper'))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.suggest_stage_id('1TheGreatPCMapper'))


class Stage(unittest.TestCase):

    def test_no_inputs_no_outputs(self):
        stage = testsuite.create_stage(id = 'test')
        data  = dict()
        cfg   = pypers.config.Config()
        dt    = stage(data, cfg, out = 'muted')
        self.assertIsInstance(dt, float)
        self.assertEqual(data, dict())

    def test_init(self):
        class Stage(pypers.pipeline.Stage):
            pass
        self.assertEqual(Stage().id, 'stage')

    def test(self):
        stage = testsuite.create_stage(id = 'test', inputs = ['x1', 'x2'], outputs = ['y'], \
            process = lambda x1, x2, cfg, log_root_dir = None, out = None: \
                dict(y = \
                    x1 * cfg.get('x1_factor', 0) + \
                    x2 * cfg.get('x2_factor', 0))
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
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda x, cfg, log_root_dir = None, out = None: \
                dict(y = x)
            )
        data = dict(x = 0)
        cfg  = pypers.config.Config()
        self.assertRaises(TypeError, lambda: stage(data, cfg, out = 'muted'))

    def test_missing_output(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda cfg, log_root_dir = None, out = None: \
                dict()
            )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))

    def test_spurious_output(self):
        stage = testsuite.create_stage( id = 'test', \
            process = lambda cfg, log_root_dir = None, out = None: \
                dict(y = 0)
            )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))

    def test_missing_and_spurious_output(self):
        stage = testsuite.create_stage(id = 'test', outputs = ['y'], \
            process = lambda cfg, log_root_dir = None, out = None: \
                dict(z = 0)
            )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))

    def test_consumes(self):
        stage = testsuite.create_stage(id = 'test', consumes = ['x'], \
            process = lambda x, cfg, log_root_dir = None, out = None: \
                dict()
            )
        data = dict(x = 0, y = 1)
        cfg  = pypers.config.Config()
        stage(data, cfg, out = 'muted')
        self.assertEqual(data, dict(y = 1))

    def test_missing_consumes(self):
        stage = testsuite.create_stage(id = 'test', consumes = ['x'], \
            process = lambda x, cfg, log_root_dir = None, out = None: \
                dict()
            )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(KeyError, lambda: stage(data, cfg, out = 'muted'))


class Pipeline(unittest.TestCase):

    def _test_configure(self, original_base_cfg: 'config.Config'):
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

    def test_configure(self):
        cfg = pypers.config.Config()
        self._test_configure(cfg)

        cfg = pypers.config.Config()
        cfg['stage1/x1_factor'] = 1
        self._test_configure(cfg)

        cfg = pypers.config.Config()
        cfg['stage1/AF_x1_factor'] = 1
        self._test_configure(cfg)

    def test_find(self):
        pipeline = create_pipeline().test()
        for expected_position, stage in enumerate(pipeline.stages):
            with self.subTest(expected_position = expected_position):
                actual_position = pipeline.find(stage.id)
                self.assertEqual(actual_position, expected_position)

    def test_find_missing(self):
        pipeline = create_pipeline().test()
        dummy = object()
        self.assertIs(pipeline.find('stage4', dummy), dummy)

    def test_append(self):
        new_stage = testsuite.create_stage(id = 'new_stage')
        pipeline = create_pipeline().test()
        expected_pos = len(pipeline.stages)
        pos = pipeline.append(new_stage)
        self.assertEqual(pos, expected_pos)
        self.assertEqual(len(pipeline.stages), expected_pos + 1)
        self.assertIs(pipeline.stages[pos], new_stage)

    def test_append_twice(self):
        pipeline = create_pipeline().test()
        for stage in pipeline.stages:
            with self.subTest(stage = stage.id):
                self.assertRaises(RuntimeError, lambda: pipeline.append(stage))
                self.assertRaises(RuntimeError, lambda: pipeline.append(testsuite.create_stage(id = stage.id)))

    def test_append_after_str(self):
        new_stage = testsuite.create_stage(id = 'new_stage')
        stages = create_pipeline().test().stages
        for after in [stage.id for stage in stages]:
            pipeline = create_pipeline().test()
            expected_pos = pipeline.find(after) + 1
            with self.subTest(after = after):
                pos = pipeline.append(new_stage, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], new_stage)

    def test_append_after_int(self):
        new_stage = testsuite.create_stage(id = 'new_stage')
        stages = create_pipeline().test().stages
        for after in range(-1, len(stages)):
            pipeline = create_pipeline().test()
            expected_pos = after + 1
            with self.subTest(after = after):
                pos = pipeline.append(new_stage, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], new_stage)

    def test_process(self):
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
                    expected_final_cfg[f'{stage.id}/enabled'] = True
                    self.assertTrue(stage.id in timings)
                self.assertEqual(final_cfg, expected_final_cfg, f'expected: {expected_final_cfg}, actual: {final_cfg}')
                self.assertEqual(data['y'], x1_factor * input + x2_factor * input + constant)
                self.assertEqual(len(timings), len(pipeline.stages))

    def test_process_first_stage(self):
        cfg = pypers.config.Config()
        cfg['stage1/x1_factor'] = 1
        cfg['stage2/x2_factor'] = 2
        cfg['stage3/constant' ] = 3
        pipeline = create_pipeline().test()
        for stage in pipeline.stages:
            def _cb(stage2, name, data, out):
                pipeline.call_record.append(f'{stage2.id} {name}')
            stage.add_callback('start', _cb)
            stage.add_callback('end'  , _cb)
            stage.add_callback('skip' , _cb)
        def expected_call_record(processed_stages):
            return sum((([
                f'{stage.id} start',
                f'{stage.id} process',
                f'{stage.id} end',
            ] if stage.id in processed_stages else [
                f'{stage.id} skip',
            ]) for stage in pipeline.stages), [])
        for suffix, offset in (('', 0), ('+', 1)):
            for input in range(3):
                full_data, _, _ = pipeline.process(input = input, cfg = cfg, out = 'muted')
                self.assertEqual(pipeline.call_record, expected_call_record([stage.id for stage in pipeline.stages]))
                pipeline.call_record.clear()
                for first_stage_idx, first_stage in enumerate((stage.id for stage in pipeline.stages[:len(pipeline.stages) - offset])):
                    with self.subTest(suffix = suffix, offset = offset, input = input, first_stage = first_stage):
                        remaining_stages = frozenset([stage.id for stage in pipeline.stages[first_stage_idx + offset:]])
                        data, _, timings = pipeline.process(input = input, data = full_data, first_stage = first_stage + suffix, cfg = cfg, out = 'muted')
                        self.assertEqual(data['y'], full_data['y'])
                        self.assertEqual(frozenset(timings.keys()), remaining_stages)
                        self.assertEqual(pipeline.call_record, expected_call_record(remaining_stages))
                        pipeline.call_record.clear()

    def test_get_extra_stages(self):
        stages = [
            testsuite.create_stage(id = 'stage1', inputs = ['input'], outputs = ['x1']),
            testsuite.create_stage(id = 'stage2', inputs =    ['x1'], outputs = ['x2']),
            testsuite.create_stage(id = 'stage3', inputs =    ['x2'], outputs = ['x3']),
            testsuite.create_stage(id = 'stage4', inputs =    ['x3'], outputs = ['x4']),
        ]
        pipeline = pypers.pipeline.create_pipeline(stages)
        self.assertEqual(frozenset(pipeline.get_extra_stages(first_stage = 'stage4', last_stage = None, available_inputs =     [])), frozenset(['stage1', 'stage2', 'stage3']))
        self.assertEqual(frozenset(pipeline.get_extra_stages(first_stage = 'stage4', last_stage = None, available_inputs = ['x1'])), frozenset(['stage2', 'stage3']))
        self.assertEqual(frozenset(pipeline.get_extra_stages(first_stage = 'stage4', last_stage = None, available_inputs = ['x2'])), frozenset(['stage3']))
        self.assertEqual(frozenset(pipeline.get_extra_stages(first_stage = 'stage3', last_stage = None, available_inputs = ['x2'])), frozenset([]))

    def test_fields(self):
        expected_fields = set(['input', 'x1', 'x2', 'y'])
        self.assertEqual(create_pipeline().test().fields, expected_fields)


class create_pipeline(unittest.TestCase):

    def test(self, configure_stage1 = None, configure_stage2 = None, configure_stage3 = None):
        call_record = list()
        def _process_stage1(input, cfg, log_root_dir = None, out = None):
            call_record.append('stage1 process')
            return dict(x1 = input * cfg['x1_factor'])
        
        def _process_stage2(input, cfg, log_root_dir = None, out = None):
            call_record.append('stage2 process')
            return dict(x2 = input * cfg['x2_factor'])
        
        def _process_stage3(x1, x2, cfg, log_root_dir = None, out = None):
            call_record.append('stage3 process')
            return dict(y = x1 + x2 + cfg['constant'])
        stages = [
            ## stage1 takes `input` and produces `x1`
            testsuite.create_stage(id = 'stage1', inputs = ['input'], outputs = ['x1'], \
                process = _process_stage1, \
                configure = configure_stage1
            ),
            ## stage2 consumes `input` and produces `x2`
            testsuite.create_stage(id = 'stage2', outputs = ['x2'], consumes = ['input'], \
                process = _process_stage2,
                configure = configure_stage2
            ),
            ## stage3 takes `x1` and `x2` and produces `y`
            testsuite.create_stage(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['y'], \
                process = _process_stage3,
                configure = configure_stage3
            ),
        ]
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                pipeline = pypers.pipeline.create_pipeline(permutated_stages)
                self.assertEqual(len(pipeline.stages), 3)
                for stage1, stage2 in zip(pipeline.stages, stages):
                    self.assertIs(stage1, stage2)
        pipeline.call_record = call_record
        return pipeline

    def test_unsatisfiable(self):
        stages = [
            testsuite.create_stage(id = 'stage1', inputs = ['input'], outputs = ['x1']),     ## stage1 takes `input` and produces `x1`
            testsuite.create_stage(id = 'stage2', outputs = ['x2'], consumes = ['input']),   ## stage2 consumes `input` and produces `x2`
            testsuite.create_stage(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['y']),   ## stage3 takes `x1` and `x2` and produces `y`
            testsuite.create_stage(id = 'stage4', inputs = ['input', 'y'], outputs = ['z']), ## stage4 takes `input` and `y` and produces `z`
        ]
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                self.assertRaises(RuntimeError, lambda: pypers.pipeline.create_pipeline(permutated_stages))

    def test_ambiguous_namespaces(self):
        stage1 = testsuite.create_stage(id = 'stage1')
        stage2 = testsuite.create_stage(id = 'stage2')
        stage3 = testsuite.create_stage(id = 'stage2')
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage3]))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage2]))

    def test_ambiguous_outputs(self):
        stage1 = testsuite.create_stage(id = 'stage1', outputs = ['x1'])
        stage2 = testsuite.create_stage(id = 'stage2', outputs = ['x2'])
        stage3 = testsuite.create_stage(id = 'stage3', outputs = ['x2'])
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage3]))


if __name__ == '__main__':
    unittest.main()
