import argparse
import asyncio
import contextlib
import dill
import importlib
import os
import pathlib
import platform
import re
import sys
import traceback
import unittest

import repype.textual.app
import tests.test_repype


class Textual(unittest.IsolatedAsyncioTestCase):

    async def test(self):
        python_version = platform.python_version_tuple()
        if python_version[0] != 3 or python_version[1] < 10:
            self.skipTest('Textual tests require Python 3.10 or later')

        else:
            test_filename_pattern = re.compile(r'^test_[a-zA-Z0-9_]+\.py$')
            test_directory_path = pathlib.Path(__file__).parent / 'textual'
            for filename in os.listdir(test_directory_path):
                if test_filename_pattern.match(filename):
                    filepath = test_directory_path / filename
                    with self.subTest(test = filepath.stem):

                        test_process = await asyncio.create_subprocess_exec(
                            sys.executable,
                            '-m'
                            'tests.test_textual',
                            filepath.stem,
                            stdout = asyncio.subprocess.PIPE,
                            stderr = asyncio.subprocess.PIPE,
                        )
                        stdout, stderr = await test_process.communicate()
                        if stdout:
                            print(stdout.decode())
                        if stderr:
                            exception = dill.loads(stderr)
                            raise exception
                    

class TextualTestCase(unittest.TestCase):

    def setUp(self):
        self.repype_segmentation = tests.test_repype.repype_segmentation()
        self.repype_segmentation.setUp()
        self.app = repype.textual.app.Repype(path = self.repype_segmentation.root_path)

    def tearDown(self):
        self.repype_segmentation.tearDown()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('test', type = str)
    args = parser.parse_args()

    try:
        async def main():
            with contextlib.redirect_stderr(sys.stdout):
                test = importlib.import_module(f'tests.textual.{args.test}')
                test_case_str = test.test_case
                test_case_module = importlib.import_module('.'.join(test_case_str.split('.')[:-1]))
                test_case_class  = getattr(test_case_module, test_case_str.split('.')[-1])
                test_case = test_case_class()
                try:
                    test_case.setUp()
                    await test.run(test_case)
                finally:
                    test_case.tearDown()
        asyncio.run(main()) 
    except:
        print(traceback.format_exc())
        error_serialized = dill.dumps(sys.exc_info()[1])
        sys.stderr.buffer.write(error_serialized)