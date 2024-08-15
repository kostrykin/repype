import dill
import gzip
import hashlib
import importlib
import json
import os
import pathlib
import re

import frozendict
import repype.pipeline
import repype.config
import repype.stage
import repype.status
from repype.typing import (
    Any,
    DataDictionary,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Optional,
    PathLike,
    Self,
    TypeVar,
    Union,
)
import yaml


FileID = TypeVar('FileID', int, str)
MultiDataDictionary = Dict[FileID, DataDictionary]


def decode_inputs(spec: Union[str, List[FileID]]) -> List[FileID]:
    """
    Convert a string of comma-separated inputs (or ranges thereof) to a list of integers.

    If ``spec`` is a list already, it is returned as is.
    """
    # Convert a string of comma-separated inputs (or ranges thereof) to a list of integers
    if isinstance(spec, str):

        # Split the string by commas and remove whitespace
        inputs = list()
        for token in spec.replace(' ', '').split(','):
            if token == '':
                continue

            # Check if the token is a range of inputs
            m = re.match(r'^([0-9]+)-([0-9]+)$', token)

            # If the token is a single input, add it to the list
            if m is None and re.match(r'^[0-9]+$', token):
                inputs.append(int(token))
                continue

            # If the token is a range of inputs, add all inputs in the range to the list
            elif m is not None:
                first, last = int(m.group(1)), int(m.group(2))
                if first < last:
                    inputs += list(range(first, last + 1))
                    continue
            
            # If the token is neither a single input nor a range of inputs, raise an error
            raise ValueError(f'Cannot parse input token "{token}"')

        return sorted(frozenset(inputs))
    
    # Otherwise, treat the input as a list of inputs
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
    def inputs(self):
        return decode_inputs(self.full_spec.get('inputs', []))
    
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
    def digest_task_filepath(self):
        return self.resolve_path('.task.json')
        
    @property
    def digest_sha_filepath(self):
        return self.resolve_path('.sha.json')

    @property
    def digest(self):
        """
        Immutable full specification which this task was previously completed with (or None).
        """
        if not self.digest_task_filepath.is_file():
            return None
        with self.digest_task_filepath.open('r') as digest_task_file:
            return frozendict.deepfreeze(json.load(digest_task_file))
        
    @property
    def parents(self) -> Iterator[Self]:
        """
        Generator which yields all parent tasks of this task, starting with the immediate parent.
        """
        task = self.parent
        while task is not None:
            yield task
            task = task.parent

    def get_full_spec_with_config(self, config: repype.config.Config) -> dict:
        return self.full_spec | dict(config = config.entries)
    
    def compute_sha(self, config: Optional[repype.config.Config] = None) -> str:
        full_spec = self.full_spec if config is None else self.get_full_spec_with_config(config)
        return hashlib.sha1(json.dumps(full_spec).encode('utf8')).hexdigest()
    
    def create_config(self) -> repype.config.Config:
        """
        Creates object which represents the hyperparameters of this task.

        The hyperparameters are combined from three sources, in the following order, where the later sources take precedence:
        1. The hyperparameters of the parent task.
        2. The base config file specified in the task spec.
        2. The config section of the task spec.

        Changes to the hyperparameters are not reflected in the spec of the task.
        """
        config = repype.config.Config(self.spec.get('config', dict())).copy()

        # Load the base config file
        base_config_path = self.spec.get('base_config_path')
        if base_config_path:
            base_config_path = self.resolve_path(base_config_path)
            with base_config_path.open('r') as base_config_file:
                base_config = repype.config.Config(yaml.safe_load(base_config_file))
            
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

    def create_pipeline(self, *args, **kwargs) -> repype.pipeline.Pipeline:
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
            return repype.pipeline.create_pipeline(stages, *args, **kwargs)
    
    def is_pending(self, pipeline: repype.pipeline.Pipeline, config: repype.config.Config) -> bool:
        """
        True if the task needs to run, and False if the task is completed or not runnable.
        """
        # Non-runnable tasks never are pending
        if not self.runnable:
            return False
        
        # If the task is not completed, it is pending
        if not self.digest_sha_filepath.is_file():
            return True
        
        # Read the hashes of the completed task
        with self.digest_sha_filepath.open('r') as digest_sha_file:
            hashes = json.load(digest_sha_file)

        # If the task is completed, but the pipeline has changed, the task is pending
        for stage in pipeline.stages:
            if stage.sha != hashes['stages'][stage.id]:
                return True

        # If the task is completed, but the configuration has changed, the task is pending
        return hashes['task'] != self.compute_sha(config)
    
    def get_marginal_fields(self, pipeline: repype.pipeline.Pipeline) -> FrozenSet[str]:
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
    
    def load(self, pipeline: Optional[repype.pipeline.Pipeline] = None) -> MultiDataDictionary:
        """
        Load the previously computed data of the task.

        To ensure consistency with the task specification, it is verified that the loaded data contains results for all inputs of the task, and no additional inputs.
        If pipeline is not None, a check for consistency of the data with the pipeline is performed.
        The loaded data is consistent with the pipeline if the data contains all fields which are not marginal according to the :meth:`get_marginal_fields` method, and no additional fields.

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
        assert frozenset(data.keys()) == frozenset(self.inputs), 'Loaded data is inconsistent with task specification.'

        # Check if the data is consistent with the pipeline
        if pipeline is not None:
            required_fields = pipeline.fields - self.get_marginal_fields(pipeline)
            assert all(
                (frozenset(data[input].keys()) == required_fields for input in data.keys())
            ), 'Loaded data is inconsistent with the pipeline.'

        # Return the loaded data
        return data
    
    def strip_marginals(self, pipeline: repype.pipeline.Pipeline, data_chunk: MultiDataDictionary) -> DataDictionary:
        """
        Strip the marginal fields from the data.

        Args:
            data (dict): A dictionary of data dictionaries.
            pipeline (Pipeline): The pipeline object.

        Returns:
            dict: A dictionary of data dictionaries without the marginal fields.
        """
        marginal_fields = self.get_marginal_fields(pipeline)
        return {
            field: data_chunk[field] for field in data_chunk if field not in marginal_fields
        }
        
    def store(self, pipeline: repype.pipeline.Pipeline, data: MultiDataDictionary, config: repype.config.Config):
        """
        Store the results of the task and the metadata.
        """
        assert self.runnable
        assert frozenset(data.keys()) == frozenset(self.inputs)

        # Strip the marginal fields from the data
        data_without_marginals = {
            input: self.strip_marginals(pipeline, data[input]) for input in data
        }

        # Store the stripped data
        with gzip.open(self.data_filepath, 'wb') as data_file:
            dill.dump(data_without_marginals, data_file, byref = True)

        # Store the digest task specification
        with self.digest_task_filepath.open('w') as digest_task_file:
            json.dump(self.get_full_spec_with_config(config), digest_task_file)

        # Store the hashes
        hashes = dict(
            stages = {stage.id: stage.sha for stage in pipeline.stages},
            task = self.compute_sha(config),
        )
        with self.digest_sha_filepath.open('w') as digest_sha_file:
            json.dump(hashes, digest_sha_file)

    def find_first_diverging_stage(self, pipeline: repype.pipeline.Pipeline, config: repype.config.Config) -> Optional[repype.stage.Stage]:
        # If the task is not completed, the first diverging stage is the first stage of the pipeline
        if not self.digest_sha_filepath.is_file():
            return pipeline.stages[0]

        # Load the stages and corresponding hashes which were used to complete the task
        with self.digest_sha_filepath.open('r') as digest_sha_file:
            digest_sha = json.load(digest_sha_file)
        digest_stage_ids = digest_sha['stages'].keys()

        # Iterate the stages from first to last
        for stage in pipeline.stages:

            # Check if the stage is new
            if stage.id not in digest_stage_ids:
                return stage
            
            # Check if the stage implementation has changed
            if stage.sha != digest_sha['stages'][stage.id]:
                return stage
            
            # Check if the stage configuration has changed
            if self.digest['config'].get(stage.id, dict()) != config.get(stage.id, dict()).entries:
                return stage
            
        # There is no diverging stage
        return None
            
    def find_pickup_task(self, pipeline: repype.pipeline.Pipeline, config: repype.config.Config) -> Dict:
        candidates = list(self.parents) + [self]
        first_diverging_stages = {task: task.find_first_diverging_stage(pipeline, config) for task in candidates}

        # There are no previous tasks to pick up from
        if len(first_diverging_stages) == 0:
            return dict(
                task = None,
                first_diverging_stage = pipeline.stages[0],
            )

        # If there is any task without a diverging stage, return that one
        for pickup_task, first_diverging_stage in first_diverging_stages.items():
            if first_diverging_stage is None:
                return dict(
                    task = pickup_task,
                    first_diverging_stage = None,
                )
        
        # Find the task with the latest diverging stage
        pickup_task = max(first_diverging_stages, key = lambda task: pipeline.find(first_diverging_stages[task].id))

        # If the latest diverging stage is the first stage of the pipeline, then there is nothing to pick up from
        # Otherwise, return the determined task and the corresponding latest diverging stage
        first_diverging_stage = first_diverging_stages[pickup_task]
        return dict(
            task = None if first_diverging_stage is pipeline.stages[0] else pickup_task,
            first_diverging_stage = first_diverging_stage,
        )
    
    def run(
            self,
            config: repype.config.Config,
            pipeline: Optional[repype.pipeline.Pipeline] = None,
            pickup: bool = True,
            strip_marginals: bool = True,
            status: Optional[repype.status.Status] = None,
        ) -> MultiDataDictionary:

        assert self.runnable
        if pipeline is None:
            pipeline = self.create_pipeline()

        # Find a task and stage to pick up from
        if pickup:
            pickup_info = self.find_pickup_task(pipeline, config)
            if pickup_info['task'] is not None:
                data = pickup_info['task'].load(pipeline)
                first_stage = pickup_info['first_diverging_stage']
            else:
                pickup = False

        # If there is no task to pick up from, run the pipeline from the beginning
        if not pickup:
            data = dict()
            first_stage = None

        # Announce the status of the task
        repype.status.update(
            status = status,
            info = 'start',
            task = str(self.path.resolve()),
            pickup = str(pickup_info['task'].path.resolve()) if pickup else None,
            first_stage = first_stage.id if first_stage else None,
        )

        # Run the pipeline for all inputs
        for input_idx, input in enumerate(self.inputs):
            
            # Announce the status of the task
            repype.status.update(
                status = status,
                info = 'process',
                task = str(self.path.resolve()),
                input = input,
                step = input_idx,
                step_count = len(self.inputs),
            )

            # Process the input
            data_chunk = data.get(input, dict())
            data_chunk = pipeline.process(
                input = input,
                data = data_chunk,
                cfg = config,
                first_stage = first_stage,
            )

            if strip_marginals:
                data_chunk = self.strip_marginals(pipeline, data_chunk)

            data[input] = data_chunk

        # Store the results for later pick up
        repype.status.update(status, info = 'storing', intermediate = True)
        self.store(pipeline, data, config)
        repype.status.update(
            status = status,
            info = 'completed',
            task = str(self.path.resolve()),
        )
        return data
    
    def __repr__(self):
        config = self.create_config()
        return f'Task({self.path}, {config.sha.hexdigest()[:7]})'