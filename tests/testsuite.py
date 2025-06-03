import contextlib
import inspect
import io
import os
import pathlib
import re
import shutil
import sys
import tempfile

import repype.stage
from repype.typing import PathLike

# Losen truncation limit for error messages
try:
    __import__('sys').modules['unittest.util']._MAX_LENGTH = 1000
except KeyError:
    pass


def with_temporary_paths(count: int):
    def decorator(test_func):

        def wrapper(self, *args, **kwargs):
            paths = [tempfile.mkdtemp() for _ in range(count)]
            try:
                ret = test_func(self, *[pathlib.Path(path) for path in paths], *args, **kwargs)
            finally:
                for path in paths:
                    shutil.rmtree(path)
            return ret

        async def async_wrapper(self, *args, **kwargs):
            paths = [tempfile.mkdtemp() for _ in range(count)]
            try:
                ret = await test_func(self, *[pathlib.Path(path) for path in paths], *args, **kwargs)
            finally:
                for path in paths:
                    shutil.rmtree(path)
            return ret

        return async_wrapper if inspect.iscoroutinefunction(test_func) else wrapper
    return decorator


def with_envvars(**envvars):
    def decorator(test_func):

        def wrapper(self, *args, **kwargs):
            environ = dict(os.environ)
            os.environ.update(envvars)
            try:
                return test_func(self, *args, **kwargs)
            finally:
                os.environ.clear()
                os.environ.update(environ)

        async def async_wrapper(self, *args, **kwargs):
            environ = dict(os.environ)
            os.environ.update(envvars)
            try:
                return await test_func(self, *args, **kwargs)
            finally:
                os.environ.clear()
                os.environ.update(environ)

        return async_wrapper if inspect.iscoroutinefunction(test_func) else wrapper
    return decorator


def create_stage_class(**kwargs):
    kwargs = dict(kwargs)

    kwargs.setdefault('inputs'  , [])
    kwargs.setdefault('outputs' , [])
    kwargs.setdefault('consumes', [])

    _process   = kwargs.get('process'  , None)
    _configure = kwargs.get('configure', None)

    class DummyStage(repype.stage.Stage):

        id       = kwargs['id']
        inputs   = kwargs['inputs']
        outputs  = kwargs['outputs']
        consumes = kwargs['consumes']

        def process(self, *args, **kwargs):
            assert frozenset(self.inputs).issubset(frozenset(kwargs.keys()))
            if _process is None: return dict()
            else: return _process(*args, **kwargs)

        def configure(self, *args, **kwargs):
            if _configure is None: return dict()
            else: return _configure(*args, **kwargs)

    return DummyStage

def create_stage(**kwargs):
    stage_class = create_stage_class(**kwargs)
    return stage_class()


# Test create_stage:

_stage = create_stage(id = 'dummy', inputs = ['x1', 'x2'], outputs = ['y'])
assert isinstance(_stage, repype.stage.Stage)
assert isinstance(_stage.inputs , frozenset)
assert isinstance(_stage.outputs, frozenset)
assert _stage.inputs  == frozenset(['x1', 'x2'])
assert _stage.outputs == frozenset(['y'])
del _stage


def create_task_file(task_path: PathLike, spec_yaml: str) -> None:
    task_path = pathlib.Path(task_path)
    task_filepath = task_path / 'task.yml'
    if not task_path.is_dir():
        task_path.mkdir(parents = True, exist_ok = True)
    with task_filepath.open('w') as spec_file:
        spec_file.write(spec_yaml)


class TestError(Exception):
    
    def __ini__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class CaptureOutput:

    create_redirect = None

    def __init__(self):
        self.stdout_buf = io.StringIO()

    def __enter__(self):
        self.redirect = self.create_redirect(self.stdout_buf)
        self.redirect.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.redirect.__exit__(exc_type, exc_value, traceback)
        if exc_value is not None:
            print(str(self), file = sys.stderr)

    def __str__(self):
        return re.sub(r'\033\[K', '', self.stdout_buf.getvalue())


class CaptureStdout(CaptureOutput):

    create_redirect = contextlib.redirect_stdout
