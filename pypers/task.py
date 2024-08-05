import importlib
import pathlib
from types import SimpleNamespace
from typing import (
    Optional,
    Self,
    TypeVar,
)

import pypers.pipeline
import pypers.config
import yaml


PathLike = TypeVar('PathLike', str, pathlib.Path)


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