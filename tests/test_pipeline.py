import unittest
import pypers.config

from . import testsuite


class Stage(unittest.TestCase):

    def test_no_inputs_no_outputs(self):
        stage = testsuite.DummyStage('test', [], [], None)
        data  = dict()
        cfg   = pypers.config.Config()
        dt    = stage(data, cfg, out = 'muted')
        self.assertIsInstance(dt, float)
        self.assertEqual(data, dict())

    def test(self):
        stage = testsuite.DummyStage('test', ['x1', 'x2'], ['y'], \
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
        stage = testsuite.DummyStage('test', [], ['y'], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict(y = input_data['x'])
           )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(KeyError, lambda: stage(data, cfg, out = 'muted'))

    def test_missing_output(self):
        stage = testsuite.DummyStage('test', [], ['y'], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict()
           )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))

    def test_spurious_output(self):
        stage = testsuite.DummyStage('test', [], [], \
            lambda input_data, cfg, log_root_dir = None, out = None: \
                dict(y = 0)
           )
        data = dict()
        cfg  = pypers.config.Config()
        self.assertRaises(AssertionError, lambda: stage(data, cfg, out = 'muted'))


class Pipeline(unittest.TestCase):

    def test_Pipeline_configure(self):
        pass

    def test_Pipeline_find(self):
        pass

    def test_Pipeline_append(self):
        pass

    def test_Pipeline_process(self):
        pass


if __name__ == '__main__':
    unittest.main()
