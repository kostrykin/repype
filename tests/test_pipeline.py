import itertools
import unittest
from unittest.mock import (
    MagicMock,
    Mock,
)

import pypers.pipeline
import pypers.config

from . import testsuite


class Pipeline(unittest.TestCase):

    def _test_configure(self, original_base_cfg: pypers.config.Config):
        factors = list(range(-1, +3))
        for x1_pre_factor, x2_pre_factor in itertools.product(factors, factors):
            for x1_default_user_factor, x2_default_user_factor in itertools.product(factors, factors):
                with self.subTest(x1_pre_factor = x1_pre_factor, x2_pre_factor = x2_pre_factor, x1_default_user_factor = x1_default_user_factor, x2_default_user_factor = x2_default_user_factor):
                    base_config = original_base_cfg.copy()
                    config = create_pipeline().test(
                            lambda x1_factor_specs, x2_factor_specs: dict(x1_factor = x1_factor_specs),
                            lambda x1_factor_specs, x2_factor_specs: dict(x2_factor = x2_factor_specs),
                        ).configure(base_config,
                            x1_factor_specs = (x1_pre_factor, x1_default_user_factor),
                            x2_factor_specs = (x2_pre_factor, x2_default_user_factor),
                        )
                    self.assertEqual(base_config, original_base_cfg)
                    self.assertEqual(config['stage1/x1_factor'], base_config.get('stage1/x1_factor', x1_pre_factor * base_config.get('stage1/AF_x1_factor', x1_default_user_factor)))
                    self.assertEqual(config['stage2/x2_factor'], base_config.get('stage2/x2_factor', x2_pre_factor * base_config.get('stage2/AF_x2_factor', x2_default_user_factor)))

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
            def _cb(stage2, name, data, status):
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
                status_mock = MagicMock()
                full_data, _, _ = pipeline.process(input = input, cfg = cfg, status = status_mock)
                self.assertEqual(pipeline.call_record, expected_call_record([stage.id for stage in pipeline.stages]))
                pipeline.call_record.clear()
                for first_stage_idx, first_stage in enumerate((stage.id for stage in pipeline.stages[:len(pipeline.stages) - offset])):
                    with self.subTest(suffix = suffix, offset = offset, input = input, first_stage = first_stage):
                        remaining_stages = frozenset([stage.id for stage in pipeline.stages[first_stage_idx + offset:]])
                        status_mock = MagicMock()
                        data, _, timings = pipeline.process(input = input, data = full_data, first_stage = first_stage + suffix, cfg = cfg, status = status_mock)
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

    def test(self):
        # Define the stages, in the order they should be executed
        stages = [
            Mock(id = 'stage1', inputs = ['input'], outputs = ['x1'], consumes = []),   # stage1 takes `input` and produces `x1`
            Mock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input']),   # stage2 consumes `input` and produces `x2`
            Mock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['y'], consumes = []), # stage3 takes `x1` and `x2` and produces `y`
        ]

        # Test `create_pipeline` with all permutations of the stages
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                pipeline = pypers.pipeline.create_pipeline(permutated_stages)
                self.assertEqual(len(pipeline.stages), 3)
                for stage1, stage2 in zip(pipeline.stages, stages):
                    self.assertIs(stage1, stage2)

    def test_unsatisfiable(self):
        stages = [
            Mock(id = 'stage1', inputs = ['input'], outputs = ['x1'], consumes = []),      # stage1 takes `input` and produces `x1`
            Mock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input']),      # stage2 consumes `input` and produces `x2`
            Mock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['y'], consumes = []),    # stage3 takes `x1` and `x2` and produces `y`
            Mock(id = 'stage4', inputs = ['input', 'y'], outputs = ['z'], consumes = []),  # stage4 takes `input` and `y` and produces `z`
        ]
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                self.assertRaises(RuntimeError, lambda: pypers.pipeline.create_pipeline(permutated_stages))

    def test_ambiguous_ids(self):
        stage1 = Mock(id = 'stage1')
        stage2 = Mock(id = 'stage2')
        stage3 = Mock(id = 'stage2')
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage3]))
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage2]))

    def test_ambiguous_outputs(self):
        stage1 = Mock(id = 'stage1', outputs = ['x1'])
        stage2 = Mock(id = 'stage2', outputs = ['x2'])
        stage3 = Mock(id = 'stage3', outputs = ['x2'])
        self.assertRaises(AssertionError, lambda: pypers.pipeline.create_pipeline([stage1, stage2, stage3]))