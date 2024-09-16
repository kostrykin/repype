import itertools
import pathlib
import unittest
from unittest.mock import (
    MagicMock,
    Mock,
)

import repype.config
import repype.pipeline

from . import testsuite


class Pipeline__configure(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.Pipeline(
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
        base_config = repype.config.Config()
        config = self.pipeline.configure(base_config, 'input')
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
        base_config = repype.config.Config()
        base_config['stage1/x1_factor'] = 100
        config = self.pipeline.configure(base_config, 'input')
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
        base_config = repype.config.Config()
        base_config['stage1/AF_x1_factor'] = 10
        config = self.pipeline.configure(base_config, 'input')
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
        base_config = repype.config.Config()
        config = self.pipeline.configure(base_config, 'input')
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
        base_config = repype.config.Config()
        config = self.pipeline.configure(base_config, 'input')
        self.assertEqual(
            config.entries['stage2'],
            dict(
                AF_x2_factor = -2,
                x2_factor = 1.,
            )
        )


class Pipeline__find(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.Pipeline(
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
        self.pipeline = repype.pipeline.Pipeline(
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
            pipeline = repype.pipeline.Pipeline(self.pipeline.stages)
            expected_pos = pipeline.find(after) + 1
            with self.subTest(after = after):
                pos = pipeline.append(self.stage4, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], self.stage4)

    def test_append_after_int(self):
        for after in range(-1, len(self.pipeline.stages)):
            pipeline = repype.pipeline.Pipeline(self.pipeline.stages)
            expected_pos = after + 1
            with self.subTest(after = after):
                pos = pipeline.append(self.stage4, after = after)
                self.assertEqual(pos, expected_pos)
                self.assertIs(pipeline.stages[pos], self.stage4)


class Pipeline__process(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.Pipeline(
            [
                MagicMock(id = 'stage1', inputs = ['input_id'], outputs = ['x1'], consumes = []),  # stage1 takes `input_id` and produces `x1`
                MagicMock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input_id']),  # stage2 consumes `input_id` and produces `x2`
                MagicMock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['x3'], consumes = []),  # stage3 takes `x1` and `x2` and produces `x3`
            ]
        )
        self.pipeline.stages[0].run.side_effect = self.stage1_run
        self.pipeline.stages[1].run.side_effect = self.stage2_run
        self.pipeline.stages[2].run.side_effect = self.stage3_run

    def stage1_run(self, pipeline, data, config, status = None,  **kwargs):
        config.set_default('enabled', True)
        data['x1'] = config.get('x1_factor', 1) * data['input_id']

    def stage2_run(self, pipeline, data, config, status = None, **kwargs):
        config.set_default('enabled', True)
        data['x2'] = config.get('x2_factor', 1) * data['input_id']

    def stage3_run(self, pipeline, data, config, status = None, **kwargs):
        config.set_default('enabled', True)
        data['x3'] = data['x1'] + data['x2'] + config.get('constant', 0)

    def test(self):
        config = repype.config.Config()
        config['stage1/x1_factor'] = 1
        config['stage2/x2_factor'] = 2
        config['stage3/constant' ] = 3
        
        for input_id in range(5):
            with self.subTest(input_id = input_id):

                mock_status = MagicMock()
                data, final_config, timings = self.pipeline.process(input_id = input_id, config = config, status = mock_status)
                mock_status.assert_not_called()

                expected_final_config = config.copy()
                for stage in self.pipeline.stages:
                    expected_final_config[f'{stage.id}/enabled'] = True

                self.assertEqual(len(timings), len(self.pipeline.stages))
                self.assertEqual(
                    [stage.id in timings for stage in self.pipeline.stages],
                    [True] * len(self.pipeline.stages),
                )
                self.assertEqual(final_config, expected_final_config)
                self.assertEqual(data['x3'], config['stage1/x1_factor'] * input_id + config['stage2/x2_factor'] * input_id + config['stage3/constant'])

    def test_with_first_stage(self):
        config = repype.config.Config()
        config['stage1/x1_factor'] = 1
        config['stage2/x2_factor'] = 2
        config['stage3/constant' ] = 3
        
        # Test combinations of `first_stage` with and without the `+` suffix
        for suffix, offset in (('', 0), ('+', 1)):

            # Test different input identifiers
            for input_id in [-1, 0, +1, +2]:

                # Pre-compute the full data
                mock_status = MagicMock()
                full_data, _, _ = self.pipeline.process(input_id = input_id, config = config, status = mock_status)
                mock_status.assert_not_called()

                # Test each stage as a `first_stage`
                for first_stage_idx, first_stage in enumerate((stage.id for stage in self.pipeline.stages[:len(self.pipeline.stages) - offset])):
                    with self.subTest(suffix = suffix, offset = offset, input_id = input_id, first_stage = first_stage):
                        remaining_stages = frozenset([stage.id for stage in self.pipeline.stages[first_stage_idx + offset:]])

                        # Reset the call records
                        for stage in self.pipeline.stages:
                            stage.reset_mock()

                        # Process the pipeline
                        mock_status = MagicMock()
                        data, _, timings = self.pipeline.process(
                            input_id = input_id,
                            data = full_data,
                            first_stage = first_stage + suffix,
                            config = config,
                            status = mock_status,
                        )
                        mock_status.assert_not_called()

                        # Check the results
                        self.assertEqual(data, full_data)
                        self.assertEqual(frozenset(timings.keys()), remaining_stages)

                        # Verify that the skipped stages were not called
                        for stage_idx, stage in enumerate(self.pipeline.stages):
                            if stage_idx < first_stage_idx + offset:
                                stage.run.assert_not_called()
                            else:
                                stage.run.assert_called_once()

    def test_with_first_stage_and_missing_input_ids(self):
        config = repype.config.Config()
        config['stage1/x1_factor'] = 1
        config['stage2/x2_factor'] = 2
        config['stage3/constant' ] = 3

        # Pre-compute the full data
        mock_status = MagicMock()
        full_data, _, _ = self.pipeline.process(input_id = 10, config = config, status = mock_status)
        mock_status.assert_not_called()

        # Test each stage as a `first_stage`
        for first_stage_idx, first_stage in enumerate((stage.id for stage in self.pipeline.stages[:len(self.pipeline.stages)])):
            remaining_stages = frozenset([stage.id for stage in self.pipeline.stages[first_stage_idx:]])

            # Remove the results from one of the previous stages
            for marginal_stage in self.pipeline.stages[:first_stage_idx]:
                full_data_without_marginals = {key: value for key, value in full_data.items() if key not in marginal_stage.outputs}
                with self.subTest(first_stage = first_stage, marginal_stage = marginal_stage.id):

                    # Reset the call records
                    for stage in self.pipeline.stages:
                        stage.reset_mock()

                    # Process the pipeline
                    mock_status = MagicMock()
                    data, _, timings = self.pipeline.process(
                        input_id = full_data['input_id'],
                        data = full_data_without_marginals,
                        first_stage = first_stage,
                        config = config,
                        status = mock_status,
                    )
                    mock_status.assert_not_called()

                    # Check the results
                    self.assertEqual(data, full_data)
                    self.assertEqual(frozenset(timings.keys()), remaining_stages | {marginal_stage.id})

                    # Verify that the skipped stages were not called
                    for stage_idx, stage in enumerate(self.pipeline.stages):
                        if stage_idx < first_stage_idx and stage.id != marginal_stage.id:
                            stage.run.assert_not_called()
                        else:
                            stage.run.assert_called_once()


class Pipeline__get_extra_stages(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.Pipeline(
            [
                MagicMock(id = 'stage1', inputs = ['input_id'], outputs = ['x1'], consumes = []),  # stage1 takes `input_id` and produces `x1`
                MagicMock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input_id']),  # stage2 consumes `input_id` and produces `x2`
                MagicMock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['x3'], consumes = []),  # stage3 takes `x1` and `x2` and produces `x3`
            ]
        )

    def test(self):
        self.assertEqual(
            sorted(self.pipeline.get_extra_stages(first_stage = 'stage1', last_stage = None, available_inputs = ['input_id'])),
            [],
        )
        self.assertEqual(
            sorted(self.pipeline.get_extra_stages(first_stage = 'stage3', last_stage = None, available_inputs = ['input_id'])),
            ['stage1', 'stage2'],
        )
        self.assertEqual(
            sorted(self.pipeline.get_extra_stages(first_stage = 'stage3', last_stage = None, available_inputs = ['input_id', 'x2'])),
            ['stage1'],
        )
        self.assertEqual(
            sorted(self.pipeline.get_extra_stages(first_stage = 'stage3', last_stage = None, available_inputs = ['input_id', 'x1'])),
            ['stage2'],
        )
        self.assertEqual(
            sorted(self.pipeline.get_extra_stages(first_stage = 'stage3', last_stage = None, available_inputs = ['input_id', 'x1', 'x2'])),
            [],
        )


class Pipeline__fields(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.Pipeline(
            [
                MagicMock(id = 'stage1', inputs = ['input_id'], outputs = ['x1'], consumes = []),  # stage1 takes `input_id` and produces `x1`
                MagicMock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input_id']),  # stage2 consumes `input_id` and produces `x2`
                MagicMock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['x3'], consumes = []),  # stage3 takes `x1` and `x2` and produces `x3`
            ]
        )

    def test_fields(self):
        self.assertEqual(
            sorted(self.pipeline.fields),
            ['input_id', 'x1', 'x2', 'x3'],
        )


class Pipeline__resolve(unittest.TestCase):

    def setUp(self):
        self.pipeline = repype.pipeline.Pipeline()

    def test_relative(self):
        self.pipeline.scopes = {
            'input': 'input-%02d.json',
        }
        filepath = self.pipeline.resolve('input', 0)
        self.assertIsInstance(filepath, pathlib.Path)
        self.assertEqual(filepath, pathlib.Path.cwd() / 'input-00.json')

    @testsuite.with_temporary_paths(1)
    def test_absolute(self, path):
        self.pipeline.scopes = {
            'input': str(path / 'input-%s.json'),
        }
        filepath = self.pipeline.resolve('input', '00')
        self.assertIsInstance(filepath, pathlib.Path)
        self.assertEqual(filepath, path.resolve() / 'input-00.json')


class create_pipeline(unittest.TestCase):

    def test(self):
        # Define the stages, in the order they should be executed
        stages = [
            Mock(id = 'stage1', inputs = ['input_id'], outputs = ['x1'], consumes = []),  # stage1 takes `input_id` and produces `x1`
            Mock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input_id']),  # stage2 consumes `input_id` and produces `x2`
            Mock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['y'], consumes = []),   # stage3 takes `x1` and `x2` and produces `y`
        ]

        # Test `create_pipeline` with all permutations of the stages
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                pipeline = repype.pipeline.create_pipeline(permutated_stages)
                self.assertEqual(len(pipeline.stages), 3)
                for stage1, stage2 in zip(pipeline.stages, stages):
                    self.assertIs(stage1, stage2)

    def test_unsatisfiable(self):
        stages = [
            Mock(id = 'stage1', inputs = ['input_id'], outputs = ['x1'], consumes = []),      # stage1 takes `input_id` and produces `x1`
            Mock(id = 'stage2', inputs = [], outputs = ['x2'], consumes = ['input_id']),      # stage2 consumes `input_id` and produces `x2`
            Mock(id = 'stage3', inputs = ['x1', 'x2'], outputs = ['y'], consumes = []),       # stage3 takes `x1` and `x2` and produces `y`
            Mock(id = 'stage4', inputs = ['input_id', 'y'], outputs = ['z'], consumes = []),  # stage4 takes `input_id` and `y` and produces `z`
        ]
        for permutated_stages in itertools.permutations(stages):
            with self.subTest(permutation = permutated_stages):
                self.assertRaises(RuntimeError, lambda: repype.pipeline.create_pipeline(permutated_stages))

    def test_ambiguous_ids(self):
        stage1 = Mock(id = 'stage1')
        stage2 = Mock(id = 'stage2')
        stage3 = Mock(id = 'stage2')
        self.assertRaises(AssertionError, lambda: repype.pipeline.create_pipeline([stage1, stage2, stage3]))
        self.assertRaises(AssertionError, lambda: repype.pipeline.create_pipeline([stage1, stage2, stage2]))

    def test_ambiguous_outputs(self):
        stage1 = Mock(id = 'stage1', outputs = ['x1'])
        stage2 = Mock(id = 'stage2', outputs = ['x2'])
        stage3 = Mock(id = 'stage3', outputs = ['x2'])
        self.assertRaises(AssertionError, lambda: repype.pipeline.create_pipeline([stage1, stage2, stage3]))