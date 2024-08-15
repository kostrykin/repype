import itertools
import unittest
from unittest.mock import (
    MagicMock,
    Mock,
)

import pypers.pipeline
import pypers.config

from . import testsuite


class Pipeline__configure(unittest.TestCase):

    def setUp(self):
        self.pipeline = pypers.pipeline.Pipeline(
            [
                MagicMock(id = 'stage1'),
                MagicMock(id = 'stage2'),
            ]
        )
        self.stage1, self.stage2 = self.pipeline.stages[:2]
        self.stage1.configure.return_value = dict(
            x1_factor = (
                1,  # fixed factor
                2,  # default value for `AF_x1_factor`
            ),
        )
        self.stage2.configure.return_value = dict(
            x2_factor = (
                -1, # fixed factor
                1,  # default value for `AF_x2_factor`
            ),
        )

    def test_configure(self):
        base_config = pypers.config.Config()
        config = self.pipeline.configure(base_config)
        self.assertEqual(
            config.entries,
            dict(
                stage1 = dict(
                    AF_x1_factor = 2,
                    x1_factor = 2,
                ),
                stage2 = dict(
                    AF_x2_factor = 1,
                    x2_factor = -1,
                ),
            )
        )

    def test_configure_x1_factor(self):
        base_config = pypers.config.Config()
        base_config['stage1/x1_factor'] = 100
        config = self.pipeline.configure(base_config)
        self.assertEqual(
            config.entries,
            dict(
                stage1 = dict(
                    AF_x1_factor = 2,
                    x1_factor = 100,
                ),
                stage2 = dict(
                    AF_x2_factor = 1,
                    x2_factor = -1,
                ),
            )
        )

    def test_configure_AF_x1_factor(self):
        base_config = pypers.config.Config()
        base_config['stage1/AF_x1_factor'] = 10
        config = self.pipeline.configure(base_config)
        self.assertEqual(
            config.entries,
            dict(
                stage1 = dict(
                    AF_x1_factor = 10,
                    x1_factor = 10,
                ),
                stage2 = dict(
                    AF_x2_factor = 1,
                    x2_factor = -1,
                ),
            )
        )

    def test_configure_type_min(self):
        self.stage2.configure.return_value = dict(
            x2_factor = (
                -1, # fixed factor
                1,  # default value for `AF_x2_factor`
                dict(
                    type = float,
                    min = 0,
                    max = 1,
                ),
            ),
        )
        base_config = pypers.config.Config()
        config = self.pipeline.configure(base_config)
        self.assertEqual(
            config.entries['stage2'],
            dict(
                AF_x2_factor = 1,
                x2_factor = 0.,
            )
        )

    def test_configure_type_max(self):
        self.stage2.configure.return_value = dict(
            x2_factor = (
                -1, # fixed factor
                -2,  # default value for `AF_x2_factor`
                dict(
                    type = float,
                    min = 0,
                    max = 1,
                ),
            ),
        )
        base_config = pypers.config.Config()
        config = self.pipeline.configure(base_config)
        self.assertEqual(
            config.entries['stage2'],
            dict(
                AF_x2_factor = -2,
                x2_factor = 1.,
            )
        )


class Pipeline__find(unittest.TestCase):

    def setUp(self):
        self.pipeline = pypers.pipeline.Pipeline(
            [
                MagicMock(id = 'stage1'),
                MagicMock(id = 'stage2'),
                MagicMock(id = 'stage3'),
                MagicMock(id = 'stage4'),
            ]
        )

    def test_find(self):
        for expected_position, stage in enumerate(self.pipeline.stages):
            with self.subTest(expected_position = expected_position):
                actual_position = self.pipeline.find(stage.id)
                self.assertEqual(actual_position, expected_position)

    def test_find_missing(self):
        dummy = object()
        self.assertIs(self.pipeline.find('stage5', dummy), dummy)


class Pipeline__append(unittest.TestCase):

    def setUp(self):
        self.pipeline = pypers.pipeline.Pipeline(
            [
                MagicMock(id = 'stage1'),
                MagicMock(id = 'stage2'),
                MagicMock(id = 'stage3'),
            ]
        )
        self.stage4 = MagicMock(id = 'stage4')

    def test_append(self):
        expected_pos = len(self.pipeline.stages)
        pos = self.pipeline.append(self.stage4)
        self.assertEqual(pos, expected_pos)
        self.assertEqual(len(self.pipeline.stages), expected_pos + 1)
        self.assertIs(self.pipeline.stages[pos], self.stage4)

    def test_append_twice(self):
        for stage in self.pipeline.stages:
            with self.subTest(stage = stage.id):
                with self.assertRaises(RuntimeError):
                    self.pipeline.append(stage)
                with self.assertRaises(RuntimeError):
                    self.pipeline.append(MagicMock(id = stage.id))

    def test_append_after_str(self):
        for after in [stage.id for stage in self.pipeline.stages]:
            pipeline = pypers.pipeline.Pipeline(self.pipeline.stages)
            expected_pos = pipeline.find(after) + 1
            with self.subTest(after = after):
                pos = pipeline.append(self.stage4, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], self.stage4)

    def test_append_after_int(self):
        for after in range(-1, len(self.pipeline.stages)):
            pipeline = pypers.pipeline.Pipeline(self.pipeline.stages)
            expected_pos = after + 1
            with self.subTest(after = after):
                pos = pipeline.append(self.stage4, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], self.stage4)


class Pipeline__process(unittest.TestCase):

    def setUp(self):
        self.pipeline = pypers.pipeline.Pipeline(
            [
                MagicMock(id = 'stage1', inputs = ['input'], outputs = ['x1'], consumes = []),    # stage1 takes `input` and produces `x1`
                MagicMock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input']),    # stage2 consumes `input` and produces `x2`
                MagicMock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['x3'], consumes = []), # stage3 takes `x1` and `x2` and produces `x3`
            ]
        )
        self.pipeline.stages[0].side_effect = self.stage1
        self.pipeline.stages[1].side_effect = self.stage2
        self.pipeline.stages[2].side_effect = self.stage3

    def stage1(self, data, config, status = None, log_root_dir = None, **kwargs):
        config.set_default('enabled', True)
        data['x1'] = config.get('x1_factor', 1) * data['input']

    def stage2(self, data, config, status = None, log_root_dir = None, **kwargs):
        config.set_default('enabled', True)
        data['x2'] = config.get('x2_factor', 1) * data['input']

    def stage3(self, data, config, status = None, log_root_dir = None, **kwargs):
        config.set_default('enabled', True)
        data['x3'] = data['x1'] + data['x2'] + config.get('constant', 0)

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        config = pypers.config.Config()
        config['stage1/x1_factor'] = 1
        config['stage2/x2_factor'] = 2
        config['stage3/constant' ] = 3
        
        for input in range(5):
            with self.subTest(input = input):

                status = pypers.status.Status(path = path)
                data, final_config, timings = self.pipeline.process(input = input, config = config, status = status)

                expected_final_config = config.copy()
                for stage in self.pipeline.stages:
                    expected_final_config[f'{stage.id}/enabled'] = True

                self.assertEqual(len(timings), len(self.pipeline.stages))
                self.assertEqual(
                    [stage.id in timings for stage in self.pipeline.stages],
                    [True] * len(self.pipeline.stages),
                )
                self.assertEqual(final_config, expected_final_config)
                self.assertEqual(data['x3'], config['stage1/x1_factor'] * input + config['stage2/x2_factor'] * input + config['stage3/constant'])

"""
    def test_with_first_stage(self):
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
"""

"""
class Pipeline__get_extra_stages(unittest.TestCase):  # TODO: Refactor

    def test(self):
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
"""


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