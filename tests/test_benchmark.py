import pathlib
import tempfile
import unittest

import pandas as pd

import repype.benchmark

from . import testsuite


class Benchmark__init__(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test__new__float(self, dirpath):
        benchmark = repype.benchmark.Benchmark[float](dirpath / 'benchmark.csv')
        pd.testing.assert_frame_equal(benchmark.df, pd.DataFrame())

    @testsuite.with_temporary_paths(1)
    def test__loaded__float(self, dirpath):
        df = pd.DataFrame()
        df.at['stage1', 'input-1'] = 10
        df.to_csv(dirpath / 'benchmark.csv')
        benchmark = repype.benchmark.Benchmark[float](dirpath / 'benchmark.csv')
        pd.testing.assert_frame_equal(benchmark.df, df)


class Benchmark__save(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test__float(self, dirpath):
        benchmark = repype.benchmark.Benchmark[float](dirpath / 'benchmark.csv')
        benchmark.df.at['stage1', 'input-1'] = 10
        benchmark.save()
        df = pd.read_csv(benchmark.filepath, index_col = 0)
        pd.testing.assert_frame_equal(benchmark.df, df)


class Benchmark_set(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test__float(self, dirpath):
        benchmark1 = repype.benchmark.Benchmark[float](dirpath / 'benchmark1.csv')
        benchmark2 = repype.benchmark.Benchmark[float](dirpath / 'benchmark2.csv')
        benchmark1['stage1', 'input-1'] = 10
        ret = benchmark2.set(benchmark1)
        benchmark1['stage1', 'input-1'] = 0
        self.assertIs(ret, benchmark2)
        self.assertEqual(benchmark2['stage1', 'input-1'], 10.0)


class Benchmark__setitem__(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test__new__float(self, dirpath):
        benchmark = repype.benchmark.Benchmark[float](dirpath / 'benchmark.csv')
        benchmark['stage1', 'input-1'] = 10
        self.assertEqual(benchmark.df.at['stage1', 'input-1'], 10.0)


class Benchmark__getitem__(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test__new__float(self, dirpath):
        benchmark = repype.benchmark.Benchmark[float](dirpath / 'benchmark.csv')
        benchmark.df.at['stage1', 'input-1'] = 10
        self.assertEqual(benchmark['stage1', 'input-1'], 10.0)


class Benchmark__retain__float(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.dirpath = pathlib.Path(self.tempdir.name)
        self.benchmark = repype.benchmark.Benchmark[float](self.dirpath / 'benchmark.csv')
        self.benchmark.df.at['stage1', 'input-1'] = 10
        self.benchmark.df.at['stage1', 'input-2'] = 20
        self.benchmark.df.at['stage1', 'input-3'] = 30
        self.benchmark.df.at['stage2', 'input-1'] = 40
        self.benchmark.df.at['stage2', 'input-2'] = 50
        self.benchmark.df.at['stage2', 'input-3'] = 60

    def tearDown(self):
        self.tempdir.cleanup()

    def test_subset(self):
        ret = self.benchmark.retain(['stage1'], ['input-2', 'input-1'])
        self.assertIs(ret, self.benchmark)
        pd.testing.assert_frame_equal(
            self.benchmark.df,
            pd.DataFrame(
                {
                    'input-1': {'stage1': 10.0},
                    'input-2': {'stage1': 20.0},
                }
            ),
        )

    def test_unrelated(self):
        ret = self.benchmark.retain(['stage1', 'stage3'], ['input-0', 'input-1', 'input-2'])
        self.assertIs(ret, self.benchmark)
        pd.testing.assert_frame_equal(
            self.benchmark.df,
            pd.DataFrame(
                {
                    'input-1': {'stage1': 10.0},
                    'input-2': {'stage1': 20.0},
                }
            ),
        )


class Benchmark__eq__(unittest.TestCase):

    @testsuite.with_temporary_paths(1)
    def test__equal__float(self, dirpath):
        benchmark1 = repype.benchmark.Benchmark[float](dirpath / 'benchmark1.csv')
        benchmark2 = repype.benchmark.Benchmark[float](dirpath / 'benchmark2.csv')
        benchmark1['stage1', 'input-1'] = 10
        benchmark2['stage1', 'input-1'] = 10
        self.assertEqual(benchmark1, benchmark2)

    @testsuite.with_temporary_paths(1)
    def test__not_equal__float(self, dirpath):
        benchmark1 = repype.benchmark.Benchmark[float](dirpath / 'benchmark1.csv')
        benchmark2 = repype.benchmark.Benchmark[float](dirpath / 'benchmark2.csv')
        benchmark1['stage1', 'input-1'] = 10
        benchmark2['stage1', 'input-1'] = 20
        self.assertNotEqual(benchmark1, benchmark2)
