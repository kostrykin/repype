import argparse
import asyncio
import contextlib
import importlib
import os
import pathlib
import platform
import re
import sys
import traceback
import types
import unittest

import repype.textual.app
import tests.test_repype


class TextualTestCase(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.repype_segmentation = tests.test_repype.repype_segmentation()
        self.repype_segmentation.setUp()
        self.app = repype.textual.app.Repype(path = self.repype_segmentation.root_path)

    async def asyncTearDown(self):
        self.repype_segmentation.tearDown()

    @property
    def root_path(self):
        return self.repype_segmentation.root_path


def load_textual_test(filename):
    test_module = importlib.import_module(f'tests.textual.{filename}')
    test_case_str = test_module.test_case
    test_case_module = importlib.import_module('.'.join(test_case_str.split('.')[:-1]))
    test_case_class  = getattr(test_case_module, test_case_str.split('.')[-1])
    return types.SimpleNamespace(
        module = test_module,
        test_case_class = test_case_class,
    )


def create_composite_textual_test_cases():

    # Gather all test files from within the 'textual' directory
    test_filename_pattern = re.compile(r'^test_[a-zA-Z0-9_]+\.py$')
    test_directory_path = pathlib.Path(__file__).parent / 'textual'
    test_filepaths = [
        test_directory_path / filename
        for filename in os.listdir(test_directory_path)
        if test_filename_pattern.match(filename)
    ]

    # Inspect all test files and gather all tests
    tests = list()
    test_def_pattern = re.compile(r'^async +def +(test(?:_[a-zA-Z0-9_]+)?)')
    for filepath in test_filepaths:
        with open(filepath, 'r') as file:
            for line in file:
                if m := test_def_pattern.match(line):
                    tests.append((filepath, m.group(1)))

    # Define the composite test case for each filepath (with the proper base class)
    test_cases = dict()
    for filepath in test_filepaths:
        test = load_textual_test(filepath.stem)
        test_cases[filepath] = type(filepath.stem, (test.test_case_class,), dict())

    # Run each test in a separate process, with coverage measuring enabled
    # This is necessary, because the method `run_test` of Textual apps can be run only once per process
    os.environ['COVERAGE_PROCESS_START'] = '.coveragerc'
    for filepath, test in tests:
        def create_test_method(filepath, test):

            # Define the test method
            async def test_method(self):

                # Skip the test if the Python version is too low
                python_version = platform.python_version_tuple()
                if int(python_version[0]) != 3 or int(python_version[1]) < 10:
                    self.skipTest(f'Textual tests require Python 3.10 or later (found: {platform.python_version()})')
                    return

                # Spawn the separate test process
                test_process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    '-m'
                    'tests.test_textual',
                    filepath.stem,
                    test,
                    stdout = asyncio.subprocess.PIPE,
                    stderr = asyncio.subprocess.PIPE,
                )

                # Wait for the test process to finish
                stdout, stderr = await test_process.communicate()
                if stdout:
                    print(stdout.decode())
                if stderr:
                    self.fail(stderr.decode())

            return test_method

        # Add the test method to the composite test case
        test_case = test_cases[filepath]
        setattr(test_case, test, create_test_method(filepath, test))

    return test_cases.values()


# Register the composite test cases
for composite_test_case in create_composite_textual_test_cases():
    globals()[composite_test_case.__name__] = composite_test_case


# This script is run by the `Textual` test case in a separate process
if __name__ == '__main__':

    # Parse the test name from the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type = str)
    parser.add_argument('test', type = str)
    args = parser.parse_args()

    try:
        with contextlib.redirect_stderr(sys.stdout):
            
            # Enable coverage measurement (if available)
            try:
                import coverage
                coverage.process_startup()
            except ImportError:
                pass

            # Define the co-routine that runs the test
            async def main():

                # Load the test
                test = load_textual_test(args.filename)
                test_case = test.test_case_class()

                # Run the test with the demanded test case
                try:
                    await test_case.asyncSetUp()
                    await getattr(test.module, args.test)(test_case)
                finally:
                    await test_case.asyncTearDown()
                    
            # Fire up an event loop and run the test co-routine
            asyncio.run(main()) 

    except:
        print(traceback.format_exc())
        sys.stderr.write(str(sys.exc_info()[1]))