import importlib
import pathlib
import tempfile
from typing import (
    Optional,
    Self,
    TypeVar,
)

import pypers.pipeline


PathLike = TypeVar('PathLike', str, pathlib.Path)


class Task:

    def __init__(self, path: PathLike, spec: dict, parent: Optional[Self] = None):
        self.spec = (parent.spec | spec) if parent else spec
        self.parent = parent
        self.path = path

    def create_pipeline(self, *args, **kwargs) -> pypers.pipeline.Pipeline:
        pipeline_name = self.spec.get('pipeline')
        assert pipeline_name is not None
        module_name, class_name = pipeline_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        pipeline_class = getattr(module, class_name)
        return pipeline_class(*args, **kwargs)
    

class Batch:

    def __init__(self):
        self.tasks = []