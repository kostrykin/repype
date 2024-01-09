import unittest
import time
import io
import contextlib
import re

import pypers.output


class get_output_Test(unittest.TestCase):

    def test_get_output(self):
        out1 = pypers.output.get_output(None)
        self.assertIsInstance(out1, pypers.output.Output, f'Actual type: {type(out1)}')
        self.assertFalse(out1.muted)
        out2 = pypers.output.get_output(out1)
        self.assertIsInstance(out2, pypers.output.Output, f'Actual type: {type(out2)}')
        self.assertIs(out1, out2)
        out3 = pypers.output.get_output('muted')
        self.assertIsInstance(out3, pypers.output.Output, f'Actual type: {type(out3)}')
        self.assertTrue(out3.muted)


class DummyOutput(pypers.output.Output):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lines = list()
        self._intermediate = None

    def derive(self, muted = False, margin = 0):
        return DummyOutput(self, muted, self.margin + margin)
    
    @property
    def root(self):
        return self.parent.root if self.parent is not None else self
    
    def marginify(self, line):
        return ' ' * self.margin + line
    
    def intermediate(self, line):
        if self.muted: return
        self.root._intermediate = self.marginify(line)
    
    def write(self, line):
        if self.muted: return
        self.root._intermediate = None
        self.root.lines.append(self.marginify(line))

    def __str__(self):
        lines = self.root.lines if self.root._intermediate is None else self.root.lines + [self.root._intermediate]
        return '\n'.join(lines)
    

class OutputTest(unittest.TestCase):

    def setUp(self):
        self.out = DummyOutput()
    
    def tearDown(self):
        pass

    def test_DummyOutput(self):  ## In order to test Output.progress (see below), we need to test DummyOutput first.
        self.out.write('line 1')
        self.out.write('line 2')
        self.assertEqual(str(self.out), 'line 1\nline 2')
        self.out.intermediate('line 3')
        self.assertEqual(str(self.out), 'line 1\nline 2\nline 3')
        self.out.write('line 4')
        self.assertEqual(str(self.out), 'line 1\nline 2\nline 4')
        self.out.intermediate('line 5')
        self.assertEqual(str(self.out), 'line 1\nline 2\nline 4\nline 5')

        out2 = self.out.derive(muted=True)
        out2.intermediate('line 6')
        out2.write('line 7')
        out2.intermediate('line 8')
        self.assertEqual(str(self.out), 'line 1\nline 2\nline 4\nline 5')

        out3 = self.out.derive(muted=False, margin=2)
        out3.intermediate('line 6')
        out3.write('line 7')
        out3.intermediate('line 8')
        self.assertEqual(str(self.out), 'line 1\nline 2\nline 4\n  line 7\n  line 8')

        out4 = out2.derive(muted=False)
        out4.write('line 9')
        self.assertEqual(str(self.out), 'line 1\nline 2\nline 4\n  line 7\n  line 8')

    def test_progress_permanent(self):
        expected_output = [
            'Progress… [     ] 0.0% (0 / 5)',
            'Progress… [=    ] 20.0% (1 / 5, ETA: 00:04)',
            'Progress… [==   ] 40.0% (2 / 5, ETA: 00:03)',
            'Progress… [===  ] 60.0% (3 / 5, ETA: 00:02)',
            'Progress… [==== ] 80.0% (4 / 5, ETA: 00:01)',
            'Progress: 5 / 5, 00:05'
        ]

        actual_output = list()
        for _ in self.out.progress(expected_output[1:], text='Progress', progressbar=5, permanent=True):
            actual_output.append(str(self.out))
            time.sleep(1)
        actual_output.append(str(self.out))
        self.assertEqual(actual_output, expected_output)

    def test_progress_nonpermanent(self):
        progress_generator = self.out.progress(range(6), text='Progress', progressbar=None, permanent=False)
        for _ in progress_generator:
            pass
        self.assertEqual(str(self.out), '')


class ConsoleOutputTest(unittest.TestCase):

    def setUp(self):
        self.out = pypers.output.ConsoleOutput()
        self.out_str_buf = io.StringIO()
        self.ctx = contextlib.redirect_stdout(self.out_str_buf)
        self.ctx.__enter__()

    @property
    def out_str(self):
        return re.sub(r'\033\[K', '', self.out_str_buf.getvalue())
        #return '\n'.join([line.rstrip() for line in text.split('\n')])
    
    def tearDown(self):
        self.ctx.__exit__(None, None, None)

    def test_xxx(self):
        self.out.write('line 1')
        self.assertEqual(self.out_str, 'line 1\n')

        self.out.write('line 2')
        self.assertEqual(self.out_str, 'line 1\nline 2\n')

        out2 = self.out.derive(muted=True)
        out2.write('line 3')
        self.assertEqual(self.out_str, 'line 1\nline 2\n')

        out2 = self.out.derive(muted=False, margin=2)
        out2.write('line 4\nline 5')
        self.assertEqual(self.out_str, 'line 1\nline 2\n  line 4\n  line 5\n')

if __name__ == '__main__':
    unittest.main()