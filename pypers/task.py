import importlib
from typing import (
    Optional,
    Self,
)


class Task:

    def __init__(self, spec: dict, parent: Optional[Self] = None):
        self.spec = (parent.spec | spec) if parent else spec
        self.parent = parent

    def create_pipeline(self, *args, **kwargs):
        pipeline_name = self.spec.get('pipeline')
        assert pipeline_name is not None
        module_name, class_name = pipeline_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        pipeline_class = getattr(module, class_name)
        return pipeline_class(*args, **kwargs)