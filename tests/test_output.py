import unittest

import pypers.output


class output(unittest.TestCase):

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


if __name__ == '__main__':
    unittest.main()
