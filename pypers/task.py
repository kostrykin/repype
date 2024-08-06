import dill
import gzip
import importlib
import os
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

from deprecated import deprecated
import pypers.pipeline
import pypers.config
import yaml


PathLike = TypeVar('PathLike', str, pathlib.Path)
FileID = TypeVar('FileID', int, str)
DataDictionary = dict[str, Any]
MultiDataDictionary = dict[FileID, DataDictionary]


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
    

def load_from_module(name: str) -> Any:
    path = name.split('.')
    for i in range(1, len(path)):
        module_name = '.'.join(path[:-i])
        try:
            current = importlib.import_module(module_name)
            for attribute in path[-i:]:
                current = getattr(current, attribute)
            return current
        except ImportError:
            if i == len(path) - 1:
                raise
            else:
                continue


class Task:

    def __init__(self, path: PathLike, spec: dict, parent: Optional[Self] = None):
        self.spec   = spec
        self.parent = parent
        self.path   = pathlib.Path(path)

    @property
    def full_spec(self) -> dict:
        return (self.parent.full_spec | self.spec) if self.parent else self.spec
    
    @property
    def runnable(self) -> bool:
        return bool(self.full_spec.get('runnable'))
    
    @property
    def file_ids(self):
        return decode_file_ids(self.full_spec.get('file_ids', []))
    
    @property
    def root(self) -> Self:
        """
        The root task of the task tree.
        """
        return self.parent.root if self.parent else self
    
    @property
    def marginal_stages(self) -> List[str]:
        return self.full_spec.get('marginal_stages', [])
        
    @property
    def data_filepath(self):
        return self.resolve_path('data.dill.gz')
        
    @property
    def digest_json_filepath(self):
        return self.resolve_path('.digest.json')
        
    @property
    def digest_sha_filepath(self):
        return self.resolve_path('.digest.sha')
    
    def create_config(self) -> pypers.config.Config:
        """
        Creates object which represents the hyperparameters of this task.

        The hyperparameters are combined from three sources, in the following order, where the later sources take precedence:
        1. The hyperparameters of the parent task.
        2. The base config file specified in the task spec.
        2. The config section of the task spec.

        Changes to the hyperparameters are not reflected in the spec of the task.
        """
        config = pypers.config.Config(self.spec.get('config', dict())).copy()

        # Load the base config file
        base_config_path = self.spec.get('base_config_path')
        if base_config_path:
            base_config_path = self.resolve_path(base_config_path)
            with base_config_path.open('r') as base_config_file:
                base_config = pypers.config.Config(yaml.safe_load(base_config_file))
            
            # Merge the task config into the base config
            config = base_config.merge(config)

        # Merge the config obtained from the base config and the task spec into the parent config
        if self.parent:
            parent_config = self.parent.create_config()
            return parent_config.merge(config)
        else:
            return config
    
    def get_path_pattern(self, key: str, default: Optional[str] = None) -> Optional[pathlib.Path]:
        path_pattern = self.full_spec.get(key)
        if path_pattern is None:
            return self.path / default if default else None
        else:
            return self.path / path_pattern

    def resolve_path(self, path):
        """
        Resolves a path relative to the task directory.

        In addition, following placeholders are replaced:
        - ``{DIRNAME}``: The name of the task directory.
        - ``{ROOTDIR}``: The root directory of the task tree.
        """
        if path is None:
            return None
        
        # Replace placeholders in the path
        path = pathlib.Path(os.path.expanduser(str(path))
            .replace('{DIRNAME}', self.path.name)
            .replace('{ROOTDIR}', str(self.root.path)))
        
        # If the path is not absolute, treat it as relative to the task directory
        if not path.is_absolute():
            path = self.path / path
        
        # Resolve symlinks
        return path.resolve()

    def create_pipeline(self, *args, **kwargs) -> pypers.pipeline.Pipeline:
        pipeline = self.full_spec.get('pipeline')
        assert pipeline is not None
        assert isinstance(pipeline, (str, list))

        # Load the pipeline from a module
        if isinstance(pipeline, str):
            pipeline_class = load_from_module(pipeline)
            return pipeline_class(*args, **kwargs)
        
        # Create the pipeline from a list of stages
        if isinstance(pipeline, list):
            stages = list()
            for stage in pipeline:
                stage_class = load_from_module(stage)
                stages.append(stage_class())
            return pypers.pipeline.create_pipeline(stages, *args, **kwargs)
    
    def pending(self, config: pypers.config.Config) -> bool:
        """
        True if the task needs to run, and False if the task is completed or not runnable.
        """
        return self.runnable and not (self.digest_sha_filepath.exists() and self.digest_sha_filepath.read_text() == config.sha.hexdigest())
    
    def get_marginal_fields(self, pipeline: pypers.pipeline.Pipeline) -> FrozenSet[str]:
        """
        Get the marginal fields from a pipeline.

        The marginal fields are all outputs produced by marginal stages.
        Marginal stages are those stages which are listed in the :attr:`marginal_stages` property.

        Args:
            pipeline (Pipeline): The pipeline object.

        Returns:
            set: A set of marginal fields.
        """
        marginal_fields = sum((list(stage.outputs) for stage in pipeline.stages if stage.id in self.marginal_stages), list())
        return frozenset(marginal_fields)
    
    def pickup(self, pipeline: Optional[pypers.pipeline.Pipeline] = None) -> MultiDataDictionary:
        """
        Pick up the previously computed data of the task.

        To ensure consistency with the task specification, it is verified that the picked up data contains results for all file IDs, and no additional file IDs.
        If pipeline is not None, a check for consistency of the data with the pipeline is performed.
        The picked up data is consistent with the pipeline if the data contains all fields which are not marginal according to the :meth:`get_marginal_fields` method, and no additional fields.

        Args:
            pipeline (Pipeline): The pipeline object.

        Returns:
            dict: A dictionary of data dictionaries.
        """
        assert self.runnable
        assert self.data_filepath.is_file()
        with gzip.open(self.data_filepath, 'rb') as data_file:
            data = dill.load(data_file)

        # Check if the data is consistent with the task specification
        assert frozenset(data.keys()) == frozenset(self.file_ids), 'Picked up data is inconsistent with task specification.'

        # Check if the data is consistent with the pipeline
        if pipeline is not None:
            required_fields = pipeline.fields - self.get_marginal_fields(pipeline)
            assert all(
                (frozenset(data[file_id].keys()) == required_fields for file_id in data.keys())
            ), 'Picked up data is inconsistent with the pipeline.'

        # Return the picked up data
        return data
        
    def store(self, pipeline: pypers.pipeline.Pipeline, data: MultiDataDictionary, config: pypers.config.Config):
        """
        Store the results of the task and the metadata.
        """
        assert self.runnable
        assert frozenset(data.keys()) == frozenset(self.file_ids)

        # Strip the marginal fields from the data
        marginal_fields = self.get_marginal_fields(pipeline)
        data_without_marginals = {
            file_id: {
                field: data[file_id][field]
                for field in data[file_id] if field not in marginal_fields
            }
            for file_id in data
        }

        # Store the stripped data
        with gzip.open(self.data_filepath, 'wb') as data_file:
            dill.dump(data_without_marginals, data_file, byref=True)

        # Store the digest config
        with self.digest_json_filepath.open('w') as digest_json_file:
            config.dump_json(digest_json_file)

        # Store the digest hash
        with self.digest_sha_filepath.open('w') as digest_sha_file:
            digest_sha_file.write(config.sha.hexdigest())
    
    def __repr__(self):
        config = self.create_config()
        return f'Task({self.path}, {config.sha.hexdigest()[:7]})'
    

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
            assert task.spec == spec, f'{path}: Requested specification {spec} does not match previously loaded specification {task.spec}'
            return task