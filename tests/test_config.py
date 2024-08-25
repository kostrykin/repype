import unittest

import repype.config


class Config__yaml(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = 5
        self.config['stage1/sub/param1'] = 'xyz'

    def test(self):
        self.assertEqual(
            self.config.yaml,
            "stage1:" "\n"
            "  param1: 1000" "\n"
            "  sub:" "\n"
            "    param1: 'xyz'" "\n"
            "stage2:" "\n"
            "  param2: 5",
        )