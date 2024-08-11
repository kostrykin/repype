import contextlib
import io
import json
import os
import tempfile
import time
from unittest import TestCase

from pypers.status import Status, StatusReader
from . import testsuite


class Status__init(TestCase):

    def test__parent_path_none(self):
        with self.assertRaises(AssertionError):
            Status()

    @testsuite.with_temporary_paths(2)
    def test__parent_path_not_none(self, path1, path2):
        status = Status(path = path1)
        with self.assertRaises(AssertionError):
            Status(parent = status, path = path2)

    @testsuite.with_temporary_paths(1)
    def test__with_path(self, path):
        status = Status(path = path)
        self.assertEqual(status.path, path)
        self.assertIsNone(status.parent)

    @testsuite.with_temporary_paths(1)
    def test__with_parent(self, path):
        status1 = Status(path = path)
        status2 = Status(parent = status1)
        self.assertIs(status2.parent, status1)
        self.assertIsNone(status2.path)


class Status__root(TestCase):

    @testsuite.with_temporary_paths(1)
    def test__no_parent(self, path):
        status = Status(path = path)
        self.assertIs(status.root, status)

    @testsuite.with_temporary_paths(1)
    def test__with_parent(self, path):
        status1 = Status(path = path)
        status2 = Status(parent = status1)
        self.assertIs(status2.root, status1)


class Status__filepath(TestCase):

    @testsuite.with_temporary_paths(1)
    def test__no_parent(self, path):
        status = Status(path = path)
        self.assertEqual(status.filepath, path / f'{status.id}.json')

    @testsuite.with_temporary_paths(1)
    def test__with_parent(self, path):
        status1 = Status(path = path)
        status2 = Status(parent = status1)
        self.assertEqual(status2.filepath, path / f'{status2.id}.json')


class Status__write_intermediate(TestCase):

    @testsuite.with_temporary_paths(1)
    def test_write(self, path):
        status = Status(path = path)
        status.write('test1')
        status.write('test2')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, ['test1', 'test2'])

    @testsuite.with_temporary_paths(1)
    def test_write_intermediate(self, path):
        status = Status(path = path)
        status.write('write')
        status.intermediate('intermediate')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0], 'write')
            self.assertEqual(list(data[1].keys()), ['expand', 'scope'])
            self.assertEqual(data[1]['scope'], 'intermediate')
        with open(data[1]['expand']) as file:
            data = json.load(file)
            self.assertEqual(data, ['intermediate'])

    @testsuite.with_temporary_paths(1)
    def test_write_intermediate_write(self, path):
        status = Status(path = path)
        status.write('write1')
        status.intermediate('intermediate')
        status.write('write2')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, ['write1', 'write2'])


class Status__derive(TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        status = Status(path = path)
        child = status.derive()
        self.assertEqual(status.data, [dict(expand = str(child.filepath))])
        self.assertEqual(child.data, [])
        self.assertEqual(child.parent, status)


class Status__get(TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tempdir.name)

    def tearDown(self):
        self.tempdir.cleanup()
        os.chdir(self.cwd)

    @testsuite.with_temporary_paths(1)
    def test_instance(self, path):
        status1 = Status(path = path)
        status2 = Status.get(status1)
        self.assertIs(status1, status2)

    def test_none(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = Status.get()
        self.assertTrue(stdout.getvalue().startswith('Status written to: /'))
        self.assertTrue(stdout.getvalue().endswith(f'/.status/{status.id}.json\n'))
        status.update()
        self.assertEqual(os.listdir('.'), ['.status'])
        self.assertEqual(os.listdir('.status'), [f'{status.id}.json'])


class StatusReader__init(TestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.status1 = Status(path = self.tempdir.name)
        self.status1.write('write1')
        self.status2 = self.status1.derive()
        self.status2.write('write2')

    def tearDown(self):
        self.tempdir.cleanup()

    def test_intermediates(self):
        with StatusReader(self.status1.filepath) as status:
            self.assertEqual(status, ['write1', ['write2']])

            self.status2.write('write3')
            time.sleep(0.1)
            self.assertEqual(status, ['write1', ['write2', 'write3']])

            self.status2.intermediate('interm1')
            time.sleep(0.1)
            self.assertEqual(
                status,
                [
                    'write1',
                    [
                        'write2',
                        'write3',
                        dict(
                            scope = 'intermediate',
                            content = ['interm1'],
                        ),
                    ],
                ],
            )

            self.status2.intermediate('interm2')
            time.sleep(0.1)
            self.assertEqual(
                status,
                [
                    'write1',
                    [
                        'write2',
                        'write3',
                        dict(
                            scope = 'intermediate',
                            content = ['interm2'],
                        ),
                    ],
                ],
            )

    def test_parent_derive_after_intermediate(self):
        self.status2.intermediate('interm1')
        with StatusReader(self.status1.filepath) as status:
            status3 = self.status1.derive()
            status3.write('write3')
            time.sleep(0.1)
            self.assertEqual(status, ['write1', ['write2'], ['write3']])

    def test_derive_after_intermediate(self):
        self.status2.intermediate('interm1')
        with StatusReader(self.status1.filepath) as status:
            status3 = self.status2.derive()
            status3.write('write3')
            time.sleep(0.1)
            self.assertEqual(status, ['write1', ['write2', ['write3']]])

    def test_parent_write_after_intermediate(self):
        self.status2.intermediate('interm1')
        with StatusReader(self.status1.filepath) as status:
            self.status1.write('write3')
            time.sleep(0.1)
            self.assertEqual(status, ['write1', ['write2'], 'write3'])