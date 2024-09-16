import asyncio
import json
import os
import tempfile
from unittest import (
    IsolatedAsyncioTestCase,
    TestCase,
)
from unittest.mock import (
    call,
    patch,
)

import repype.status

from . import testsuite


async def wait_for_watchdog():
    timeout = float(os.environ.get('REPYPE_WATCHDOG_TIMEOUT', 0.1))
    await asyncio.sleep(timeout)


class Status__init(TestCase):

    def test__parent_path_none(self):
        with self.assertRaises(AssertionError):
            repype.status.Status()

    @testsuite.with_temporary_paths(2)
    def test__parent_path_not_none(self, path1, path2):
        status = repype.status.Status(path = path1)
        with self.assertRaises(AssertionError):
            repype.status.Status(parent = status, path = path2)

    @testsuite.with_temporary_paths(1)
    def test__with_path(self, path):
        status = repype.status.Status(path = path)
        self.assertEqual(status.path, path)
        self.assertIsNone(status.parent)

    @testsuite.with_temporary_paths(1)
    def test__with_parent(self, path):
        status1 = repype.status.Status(path = path)
        status2 = repype.status.Status(parent = status1)
        self.assertIs(status2.parent, status1)
        self.assertIsNone(status2.path)


class Status__root(TestCase):

    @testsuite.with_temporary_paths(1)
    def test__no_parent(self, path):
        status = repype.status.Status(path = path)
        self.assertIs(status.root, status)

    @testsuite.with_temporary_paths(1)
    def test__with_parent(self, path):
        status1 = repype.status.Status(path = path)
        status2 = repype.status.Status(parent = status1)
        self.assertIs(status2.root, status1)


class Status__filepath(TestCase):

    @testsuite.with_temporary_paths(1)
    def test__no_parent(self, path):
        status = repype.status.Status(path = path)
        self.assertEqual(status.filepath, path / f'{status.id}.json')

    @testsuite.with_temporary_paths(1)
    def test__with_parent(self, path):
        status1 = repype.status.Status(path = path)
        status2 = repype.status.Status(parent = status1)
        self.assertEqual(status2.filepath, path / f'{status2.id}.json')


class Status__write_intermediate(TestCase):

    @testsuite.with_temporary_paths(1)
    def test_write(self, path):
        status = repype.status.Status(path = path)
        status.write('test1')
        status.write('test2')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, ['test1', 'test2'])

    @testsuite.with_temporary_paths(1)
    def test_write_intermediate(self, path):
        status = repype.status.Status(path = path)
        status.write('write')
        status.intermediate('intermediate')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0], 'write')
            self.assertEqual(list(data[1].keys()), ['expand', 'content_type'])
            self.assertEqual(data[1]['content_type'], 'intermediate')
        with open(data[1]['expand']) as file:
            data = json.load(file)
            self.assertEqual(data, ['intermediate'])

    @testsuite.with_temporary_paths(1)
    def test_write_intermediate_write(self, path):
        status = repype.status.Status(path = path)
        status.write('write1')
        status.intermediate('intermediate')
        status.write('write2')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, ['write1', 'write2'])

    @testsuite.with_temporary_paths(1)
    def test_write_intermediate_none(self, path):
        status = repype.status.Status(path = path)
        status.write('write1')
        status.intermediate('intermediate')
        status.intermediate(None)
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, ['write1'])


class Status__derive(TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        status = repype.status.Status(path = path)
        child = status.derive()
        self.assertEqual(status.data, [dict(expand = str(child.filepath))])
        self.assertEqual(child.data, [])
        self.assertIs(child.parent, status)
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(len(data), 1)
            self.assertEqual(list(data[-1].keys()), ['expand'])
        with open(data[-1]['expand']) as file:
            data = json.load(file)
            self.assertEqual(data, [])


class Status__progress(TestCase):

    @testsuite.with_temporary_paths(1)
    def test(self, path):
        intermediate_path = None
        status = repype.status.Status(path = path)
        for item_idx, item in enumerate(status.progress(range(3), details = 'details')):

            if intermediate_path is None:
                with open(status.filepath) as file:
                    data = json.load(file)
                    intermediate_path = data[0]['expand']
                    
            with open(intermediate_path) as file:
                data = json.load(file)
                self.assertEqual(item, item_idx)
                self.assertEqual(
                    data,
                    [
                        dict(
                            info = 'progress',
                            details = 'details',
                            progress = item_idx / 3,
                            step = item_idx,
                            max_steps = 3,
                        ),
                    ],
                )

        # Verify that there have been three iterations, i.e. `item_idx = 0`, `item_idx = 1`, `item_idx = 2`
        self.assertEqual(item_idx, 2)
                    
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, list())

    @testsuite.with_temporary_paths(1)
    def test_break(self, path):
        intermediate_path = None
        status = repype.status.Status(path = path)

        for item_idx, item in (enumerate(status.progress(range(3), details = 'details'))):

            if intermediate_path is None:
                with open(status.filepath) as file:
                    data = json.load(file)
                    intermediate_path = data[0]['expand']
                    
            with open(intermediate_path) as file:
                data = json.load(file)
                self.assertEqual(item, item_idx)
                self.assertEqual(
                    data,
                    [
                        dict(
                            info = 'progress',
                            details = 'details',
                            progress = item_idx / 3,
                            step = item_idx,
                            max_steps = 3,
                        ),
                    ],
                )

            break

        # Verify that there has been one iterations, i.e. `item_idx = 0`
        self.assertEqual(item_idx, 0)
        
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, list())

    @testsuite.with_temporary_paths(1)
    def test_iterations_generator(self, path):
        status = repype.status.Status(path = path)
        def generator():
            for item in []:
                yield item
        for item in status.progress(generator(), iterations = 0):
            pass

        # Verify that there have been no iterations
        self.assertFalse('item' in locals())

    @testsuite.with_temporary_paths(1)
    def test_iterations_assertion_error(self, path):
        status = repype.status.Status(path = path)
        with self.assertRaises(AssertionError):
            for item in status.progress(range(3), iterations = 2):
                pass

        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, list())

    @testsuite.with_temporary_paths(1)
    def test_empty(self, path):
        status = repype.status.Status(path = path)
        for item in status.progress(list()):
            pass

        # Verify that there have been no iterations
        self.assertFalse('item' in locals())

        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, list())

    @testsuite.with_temporary_paths(1)
    def test_error(self, path):
        intermediate_path = None
        status = repype.status.Status(path = path)

        with self.assertRaises(testsuite.TestError):
            for item_idx, item in (enumerate(status.progress(range(3), details = 'details'))):

                if intermediate_path is None:
                    with open(status.filepath) as file:
                        data = json.load(file)
                        intermediate_path = data[0]['expand']
                        
                with open(intermediate_path) as file:
                    data = json.load(file)
                    self.assertEqual(item, item_idx)
                    self.assertEqual(
                        data,
                        [
                            dict(
                                info = 'progress',
                                details = 'details',
                                progress = item_idx / 3,
                                step = item_idx,
                                max_steps = 3,
                            ),
                        ],
                    )

                raise testsuite.TestError()

        # Verify that there has been one iterations, i.e. `item_idx = 0`
        self.assertEqual(item_idx, 0)
        
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, list())


class create(TestCase):

    def test(self):
        with repype.status.create() as status:
            self.assertIsInstance(status, repype.status.Status)
            status.write('test')
            self.assertEqual(status.filepath.read_text(), '["test"]')
        self.assertFalse(status.filepath.exists())


class update(TestCase):

    @testsuite.with_temporary_paths(1)
    def test_write_kwargs(self, path):
        status = repype.status.Status(path = path)
        repype.status.update(status, info = 'test1')
        repype.status.update(status, info = 'test2')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, [dict(info = 'test1'), dict(info = 'test2')])

    @testsuite.with_temporary_paths(1)
    def test_write_str(self, path):
        status = repype.status.Status(path = path)
        repype.status.update(status, 'test1')
        repype.status.update(status, 'test2')
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(data, ['test1', 'test2'])

    @testsuite.with_temporary_paths(1)
    def test_intermediate_dict(self, path):
        status = repype.status.Status(path = path)
        repype.status.update(status, info = 'intermediate', intermediate = True)
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(len(data), 1)
            self.assertEqual(list(data[0].keys()), ['expand', 'content_type'])
            self.assertEqual(data[0]['content_type'], 'intermediate')
        with open(data[0]['expand']) as file:
            data = json.load(file)
            self.assertEqual(data, [dict(info = 'intermediate')])

        # Clear the intermediate
        repype.status.update(status, None, intermediate = True)
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(len(data), 0)

    @testsuite.with_temporary_paths(1)
    def test_intermediate_str(self, path):
        status = repype.status.Status(path = path)
        repype.status.update(status, 'intermediate', intermediate = True)
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(len(data), 1)
            self.assertEqual(list(data[0].keys()), ['expand', 'content_type'])
            self.assertEqual(data[0]['content_type'], 'intermediate')
        with open(data[0]['expand']) as file:
            data = json.load(file)
            self.assertEqual(data, ['intermediate'])

        # Clear the intermediate
        repype.status.update(status, None, intermediate = True)
        with open(status.filepath) as file:
            data = json.load(file)
            self.assertEqual(len(data), 0)


class StatusReader__init(IsolatedAsyncioTestCase):

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.status1 = repype.status.Status(path = self.tempdir.name)
        self.status1.write('write1')
        self.status2 = self.status1.derive()
        self.status2.write('write2')

    def tearDown(self):
        self.tempdir.cleanup()

    @patch.object(repype.status.StatusReader, 'handle_new_status')
    async def test_without_intermediates(self, mock_handle_new_status):
        async with repype.status.StatusReader(self.status1.filepath) as status:
            self.assertEqual(status, ['write1', ['write2']])

            await wait_for_watchdog()
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call([0], status = 'write1', intermediate = False),
                    call([1, 0], status = 'write2', intermediate = False),
                ]
            )

            mock_handle_new_status.reset_mock()
            self.status2.write('write3')
            await wait_for_watchdog()
            self.assertEqual(status, ['write1', ['write2', 'write3']])
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call([1, 1], status = 'write3', intermediate = False),
                ]
            )

            mock_handle_new_status.reset_mock()
            status3 = self.status1.derive()
            status3.write('write4')
            await wait_for_watchdog()
            self.assertEqual(status, ['write1', ['write2', 'write3'], ['write4']])
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call([2, 0], status = 'write4', intermediate = False),
                ]
            )

            mock_handle_new_status.reset_mock()
            self.status1.write('write5')
            await wait_for_watchdog()
            self.assertEqual(status, ['write1', ['write2', 'write3'], ['write4'], 'write5'])
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call([3], status = 'write5', intermediate = False),
                ]
            )

    @patch.object(repype.status.StatusReader, 'handle_new_status')
    async def test_with_intermediates(self, mock_handle_new_status):
        async with repype.status.StatusReader(self.status1.filepath) as status:
            self.assertEqual(status, ['write1', ['write2']])

            self.status2.write('write3')
            await wait_for_watchdog()
            self.assertEqual(status, ['write1', ['write2', 'write3']])

            mock_handle_new_status.reset_mock()
            self.status2.intermediate('interm1')
            await wait_for_watchdog()
            self.assertEqual(
                status,
                [
                    'write1',
                    [
                        'write2',
                        'write3',
                        dict(
                            content_type = 'intermediate',
                            content = ['interm1'],
                        ),
                    ],
                ],
            )
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call(
                        [1, 2],
                        status = 'interm1',
                        intermediate = True,
                    ),
                ]
            )

            mock_handle_new_status.reset_mock()
            self.status2.intermediate('interm2')
            await wait_for_watchdog()
            self.assertEqual(
                status,
                [
                    'write1',
                    [
                        'write2',
                        'write3',
                        dict(
                            content_type = 'intermediate',
                            content = ['interm2'],
                        ),
                    ],
                ],
            )
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call(
                        [1, 2],
                        status = 'interm2',
                        intermediate = True,
                    ),
                ]
            )

            mock_handle_new_status.reset_mock()
            self.status2.intermediate(None)
            await wait_for_watchdog()
            self.assertEqual(
                status,
                [
                    'write1',
                    [
                        'write2',
                        'write3',
                    ],
                ],
            )
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call(
                        [1, 2],
                        status = None,
                        intermediate = True,
                    ),
                ]
            )

            mock_handle_new_status.reset_mock()
            self.status2.write('write4')
            await wait_for_watchdog()
            self.assertEqual(
                mock_handle_new_status.call_args_list,
                [
                    call(
                        [1, 2],
                        status = 'write4',
                        intermediate = False,
                    ),
                ]
            )
