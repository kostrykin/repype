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


class Config__pop(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000

    def test(self):
        self.assertEqual(self.config.pop('stage1/param1', None), 1000)
        self.assertEqual(self.config.pop('stage1/param1', None), None)
        self.assertEqual(self.config.pop('stage1/param1', 2000), 2000)


class Config__set_default(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = None

    def test(self):
        self.assertEqual(self.config.set_default('stage1/param1', 2000), 1000)
        self.assertEqual(self.config.set_default('stage2/param2', 2000), None)
        self.assertEqual(self.config.set_default('stage2/param2', 2000, override_none = True), 2000)


class Config__get(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = None

    def test(self):
        self.assertEqual(self.config.get('stage1/param1', None), 1000)
        self.assertEqual(self.config.get('stage1/param1', 2000), 1000)
        self.assertEqual(self.config.get('stage2/param2', 1000), None)
        self.assertEqual(self.config.get('stage2/param3', 2000), 2000)
        self.assertEqual(self.config.get('stage2/param3', 3000), 2000)


class Config__getitem__(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = None

    def test(self):
        self.assertEqual(self.config['stage1/param1'], 1000)
        self.assertEqual(self.config['stage2/param2'], None)

    def test__missing(self):
        with self.assertRaises(KeyError):
            self.config['stage2/param3']


class Config__contains__(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = None

    def test(self):
        self.assertTrue ('stage1/param1' in self.config)
        self.assertTrue ('stage2/param2' in self.config)
        self.assertFalse('stage2/param3' in self.config)


class Config__setitem__(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()

    def test(self):
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = None
        self.assertEqual(self.config['stage1/param1'], 1000)
        self.assertEqual(self.config['stage2/param2'], None)
        self.config['stage2/param3'] = 2000
        self.assertEqual(self.config['stage2/param3'], 2000)


class Config__update(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = None

    def test(self):
        self.assertEqual(self.config.update('stage1/param1', lambda x: x + 1), 1001)
        self.assertEqual(self.config['stage1/param1'], 1001)
        self.assertEqual(self.config.update('stage2/param2', lambda x: x + 1 if None else -1), -1)
        self.assertEqual(self.config['stage2/param2'], -1)
        self.assertEqual(self.config.update('stage2/param3', lambda x: x + 1 if None else -1), -1)
        self.assertEqual(self.config['stage2/param3'], -1)


class Config__merge(unittest.TestCase):

    def setUp(self):
        self.config1 = repype.config.Config()
        self.config1['stage1/param1'] = 1000
        self.config1['stage2/param2'] = None

        self.config2 = repype.config.Config()
        self.config2['stage1/param1'] = 2000
        self.config2['stage2/param3'] = 3000

    def test(self):
        ret = self.config1.merge(self.config2)
        self.assertIs(ret, self.config1)
        self.assertEqual(self.config1['stage1/param1'], 2000)
        self.assertEqual(self.config1['stage2/param2'], None)
        self.assertEqual(self.config1['stage2/param3'], 3000)


class Config__copy(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = None

    def test(self):
        config2 = self.config.copy()
        self.config['stage1/param1'] = 2000
        self.assertIsNot(config2, self.config)
        self.assertEqual(config2['stage1/param1'], 1000)
        self.assertEqual(config2['stage2/param2'], None)


class Config__sha(unittest.TestCase):

    def setUp(self):
        self.config1 = repype.config.Config()
        self.config1['stage1/param1'] = 1000
        self.config1['stage2/param2'] = None

        self.config2 = repype.config.Config()
        self.config2['stage1/param1'] = 1000
        self.config2['stage2/param2'] = None

        self.config3 = repype.config.Config()
        self.config3['stage1/param1'] = 2000
        self.config3['stage2/param2'] = None

    def test(self):
        self.assertEqual(self.config1.sha.hexdigest(), self.config2.sha.hexdigest())
        self.assertNotEqual(self.config1.sha.hexdigest(), self.config3.sha.hexdigest())


class Config__eq__(unittest.TestCase):

    def setUp(self):
        self.config1 = repype.config.Config()
        self.config1['stage1/param1'] = 1000
        self.config1['stage2/param2'] = None

        self.config2 = repype.config.Config()
        self.config2['stage1/param1'] = 1000
        self.config2['stage2/param2'] = None

        self.config3 = repype.config.Config()
        self.config3['stage1/param1'] = 2000
        self.config3['stage2/param2'] = None

    def test(self):
        self.assertTrue (self.config1 == self.config2)
        self.assertFalse(self.config1 == self.config3)


class Config__str__(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = 5
        self.config['stage1/sub/param1'] = 'xyz'

    def test(self):
        self.assertEqual(
            str(self.config),
            '{' '\n'
            '  "stage1": {' '\n'
            '    "param1": 1000,' '\n'
            '    "sub": {' '\n'
            '      "param1": "xyz"' '\n'
            '    }' '\n'
            '  },' '\n'
            '  "stage2": {' '\n'
            '    "param2": 5' '\n'
            '  }' '\n'
            '}',
        )


class Config__repr__(unittest.TestCase):

    def setUp(self):
        self.config = repype.config.Config()
        self.config['stage1/param1'] = 1000
        self.config['stage2/param2'] = 5
        self.config['stage1/sub/param1'] = 'xyz'

    def test(self):
        self.assertEqual(
            repr(self.config),
            "<Config, {'stage1': {'param1': 1000, 'sub': {'param1': 'xyz'}}, 'stage2': {'param2': 5}}>",
        )