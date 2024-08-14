import os
import pathlib
import shutil
import tempfile

import pypers.pipeline
from pypers.typing import (
    PathLike,
)


# Losen truncation limit for error messages
__import__('sys').modules['unittest.util']._MAX_LENGTH = 1000


def with_temporary_paths(count: int):
    def decorator(test_func):
        def wrapper(self, *args, **kwargs):
            testsuite_pid = os.getpid()
            paths = [tempfile.mkdtemp() for _ in range(count)]
            try:
                ret = test_func(self, *[pathlib.Path(path) for path in paths], *args, **kwargs)
            finally:
                if os.getpid() == testsuite_pid:
                    for path in paths:
                        shutil.rmtree(path)
            return ret
        return wrapper
    return decorator


def create_stage_class(**kwargs):
    kwargs = dict(kwargs)

    kwargs.setdefault('inputs'  , [])
    kwargs.setdefault('outputs' , [])
    kwargs.setdefault('consumes', [])

    _process   = kwargs.get('process'  , None)
    _configure = kwargs.get('configure', None)

    class DummyStage(pypers.pipeline.Stage):

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
assert isinstance(_stage, pypers.pipeline.Stage)
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
