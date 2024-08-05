import importlib
import pathlib
import re
from types import SimpleNamespace
from typing import (
    Any,
    FrozenSet,
    List,
    Optional,
    Self,
    TypeVar,
    Union,
)

import pypers.pipeline
import pypers.config
import yaml


PathLike = TypeVar('PathLike', str, pathlib.Path)
FileID = TypeVar('FileID', int, str)


def decode_file_ids(spec: Union[str, List[FileID]]) -> List[FileID]:
    # Convert a string of comma-separated file IDs (or ranges thereof) to a list of integers
    if isinstance(spec, str):

        # Split the string by commas and remove whitespace
        file_ids = list()
        for token in spec.replace(' ', '').split(','):
            if token == '':
                continue

            # Check if the token is a range of file IDs
            m = re.match(r'^([0-9]+)-([0-9]+)$', token)

            # If the token is a single file ID, add it to the list
            if m is None and re.match(r'^[0-9]+$', token):
                file_ids.append(int(token))
                continue

            # If the token is a range of file IDs, add all file IDs in the range to the list
            elif m is not None:
                first, last = int(m.group(1)), int(m.group(2))
                if first < last:
                    file_ids += list(range(first, last + 1))
                    continue
            
            # If the token is neither a single file ID nor a range of file IDs, raise an error
            raise ValueError(f'Cannot parse file ID token "{token}"')

        return sorted(frozenset(file_ids))
    
    # Otherwise, treat the input as a list of file IDs
    else:
        return sorted(frozenset(spec))


class Task:

    def __init__(self, path: PathLike, spec: dict, parent: Optional[Self] = None):
        self.spec = spec
        self.parent = parent
        self.path = pathlib.Path(path)

    @property
    def full_spec(self) -> dict:
        return (self.parent.full_spec | self.spec) if self.parent else self.spec
    
    @property
    def runnable(self) -> bool:
        return bool(self.full_spec.get('runnable'))
    
    @property
    def config(self):
        self.spec.setdefault('config', dict())
        return pypers.config.Config(self.full_spec['config'])
    
    @property
    def file_ids(self):
        return decode_file_ids(self.full_spec.get('file_ids', []))
    
    def get_path_pattern(self, key: str, default: Optional[str] = None) -> Optional[pathlib.Path]:
        path_pattern = self.full_spec.get(key)
        if path_pattern is None:
            return self.path / default if default else None
        else:
            return self.path / path_pattern

    def create_pipeline(self, *args, **kwargs) -> pypers.pipeline.Pipeline:
        pipeline_name = self.full_spec.get('pipeline')
        assert pipeline_name is not None
        module_name, class_name = pipeline_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        pipeline_class = getattr(module, class_name)
        return pipeline_class(*args, **kwargs)
    
    @property
    def config_digest(self):
        """
        Hash code of the hyperparameters of this task.
        """
        return self.config.md5.hexdigest()
    

class Batch:

    def __init__(self):
        self.tasks = dict()

    def task(self, path: PathLike, spec: Optional[dict] = None) -> Optional[Task]:
        path = pathlib.Path(path)
        task = self.tasks.get(path)

        # Using the spec argument overrides the spec file
        if spec is None:
            spec_filepath = path / 'task.yml'

            # If neither the spec argument was given, nor the spec file exists, return the previously loaded task
            if not spec_filepath.is_file():
                return task
            
            # If the spec file exists, load the spec
            with spec_filepath.open('r') as spec_file:
                spec = yaml.safe_load(spec_file)
        
        # Retrieve the parent task and instantiate the requested task
        if task is None:
            parent = self.task(path.parent) if path.parent else None
            task = Task(path = path, spec = spec, parent = parent)
            assert path not in self.tasks
            self.tasks[path] = task
            return task
        
        # Check whether the task has the right spec
        else:
            assert task.spec == spec, f'{path}: Requested spec {spec} does not match previously loaded spec {task.spec}'
            return task