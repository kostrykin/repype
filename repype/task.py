import gzip
import hashlib
import importlib
import json
import os
import pathlib
import re

import dill
import frozendict
import mergedeep
import yaml

import repype.benchmark
import repype.config
import repype.pipeline
import repype.stage
import repype.status
from repype.typing import (
    Any,
    Dict,
    FrozenSet,
    InputID,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    PathLike,
    PipelineData,
    Self,
    Union,
    get_args,
)

TaskData = Dict[InputID, PipelineData]
"""
Task data object. A dictionary with input objects as keys and *pipeline data objects* as values.
"""

PendingReason = Literal['incomplete', 'pipeline', 'specification', '']
"""
Reasons why a task is pending, or not pending at all.
"""


def decode_input_ids(spec: Union[InputID, List[InputID]]) -> List[InputID]:
    """
    Convert a string of comma-separated integers (or ranges thereof) to a list of integers.

    If `spec` is a list already, or a single input identifier, it is returned as is.
    """
    # Convert a string of comma-separated input identifiers (or ranges thereof) to a list of integers
    if isinstance(spec, str):

        # Split the string by commas and remove whitespace
        input_ids = list()
        for token in spec.replace(' ', '').split(','):
            if token == '':
                continue

            # Check if the token is a range of input identifiers
            m = re.match(r'^([0-9]+)?-([0-9]+)?$', token)

            # If the token is a single input identifier, add it to the list
            if m is None and re.match(r'^[0-9]+$', token):
                input_ids.append(int(token))
                continue

            # If the token looks like a range of integer input identifiers, ...
            elif m is not None:

                # ... and it is a valid range, add all identifiers in the range to the list
                if m.group(1) is not None and m.group(2) is not None:
                    first, last = int(m.group(1)), int(m.group(2))
                    if first < last:
                        input_ids += list(range(first, last + 1))
                        continue

                # ... but it is not a valid range, raise an error
                raise ValueError(f'Cannot parse input token "{token}"')

            # Otherwise, treat the token as a string identifier
            else:
                input_ids.append(str(token))

        return sorted(frozenset(input_ids))

    # Treat the input identifiers as a list of identifiers, if it is a list
    elif isinstance(spec, list):
        return sorted(frozenset(spec))

    # Otherwise, treat the input identifier as a single identifier
    else:
        return [spec]


def load_from_module(name: str) -> Any:
    """
    Load an object from a module.
    """
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
    """
    Task in batch processing.

    Each task can have a parent task, so each task is part of a task tree.

    Arguments:
        path: The path to the task directory.
        spec: The task specification.
        parent: The parent task of this task.
    """

    parent: Optional[Self]
    """
    The parent task of this task.
    """

    path: pathlib.Path
    """
    The path to the task directory.
    """

    spec: Dict[str, Any]
    """
    The task specification.
    """

    def __init__(self, path: PathLike, spec: dict, parent: Optional[Self] = None):
        self.spec   = spec
        self.parent = parent
        self.path   = pathlib.Path(path)

    def __eq__(self, other: object):
        return other is not None and all(
            (
                isinstance(other, type(self)),
                self.path == other.path,
                self.spec == other.spec,
                self.parent == other.parent,
            )
        )

    def __hash__(self):
        return hash((self.path, json.dumps(self.spec), self.parent))

    @property
    def full_spec(self) -> Dict[str, Any]:
        """
        The full specification of the task, including the parent task specifications.
        """
        return mergedeep.merge(dict(), self.parent.full_spec, self.spec) if self.parent else self.spec

    @property
    def runnable(self) -> bool:
        """
        `True` if the task is runnable, and `False` otherwise.
        """
        return bool(self.full_spec.get('runnable'))

    @property
    def input_ids(self):
        """
        The identifiers of the inputs of the task.
        """
        return decode_input_ids(self.full_spec.get('input_ids', []))

    @property
    def root(self) -> Self:
        """
        The root task of the task tree.
        """
        return self.parent.root if self.parent else self

    @property
    def marginal_stages(self) -> Iterator[str]:
        """
        The stages which are considered marginal.

        Outputs of marginal stages are removed from the *pipeline data objects* when storing the results of the task.
        The default implementation reads the list of marginal stages from the ``marginal_stages`` field in the task
        specification.

        Yields:
            Stage identifiers corresponding to the marginal stages.
        """
        for stage_spec in self.full_spec.get('marginal_stages', []):
            assert isinstance(stage_spec, str), f'Stage identifier must be a string (f{type(stage_spec)}).'

            # Load the stage from a module
            if '.' in stage_spec:
                stage_cls = load_from_module(stage_spec)
                yield stage_cls().id

            # Use the stage identifier directly
            else:
                yield stage_spec

    @property
    def data_filepath(self) -> pathlib.Path:
        """
        The path to the stored *task data object* of the task.
        """
        return self.resolve_path('data.dill.gz')

    @property
    def digest_task_filepath(self) -> pathlib.Path:
        """
        The path to the full task specification of the task completion.

        This is the full task specification adopted for the configuration which was used to complete the task.
        """
        return self.resolve_path('.task.json')

    @property
    def digest_sha_filepath(self) -> pathlib.Path:
        """
        The path to the hash values of the task completion.

        This contains the SHA-1 hashes of the full task specification adopted for the configuration which was used to
        complete the task, and the hashes of the stages of the pipeline.
        """
        return self.resolve_path('.sha.json')

    @property
    def digest(self) -> Optional[Mapping[str, Any]]:
        """
        Immutable full specification of the task completion (or `None`).
        """
        if not self.digest_task_filepath.is_file():
            return None
        with self.digest_task_filepath.open('r') as digest_task_file:
            return frozendict.deepfreeze(json.load(digest_task_file))

    @property
    def times_filepath(self) -> pathlib.Path:
        """
        The path to the CSV file with the run times of the task completion.
        """
        return self.resolve_path('times.csv')

    @property
    def times(self) -> repype.benchmark.Benchmark[float]:
        """
        The run times of the task completion.
        """
        return repype.benchmark.Benchmark[float](self.times_filepath)

    @property
    def parents(self) -> Iterator[Self]:
        """
        Generator which yields all parent tasks of this task, starting with the immediate parent.
        """
        task = self.parent
        while task is not None:
            yield task
            task = task.parent

    def get_full_spec_with_config(self, config: repype.config.Config) -> Dict[str, Any]:
        """
        Get the full specification of the task adopted for the `config`.
        """
        return self.full_spec | dict(config = config.entries)

    def compute_sha(self, config: Optional[repype.config.Config] = None) -> str:
        """
        Compute the SHA-1 hash of the full task specification adopted for the `config`.
        """
        full_spec = self.full_spec if config is None else self.get_full_spec_with_config(config)
        return hashlib.sha1(json.dumps(full_spec).encode('utf8')).hexdigest()

    def create_config(self) -> repype.config.Config:
        """
        Creates object which represents the hyperparameters of this task.

        The hyperparameters are combined from three sources, in the following order, where the later sources take
        precedence:

        #. The hyperparameters of the parent task.
        #. The ``base_config`` file specified in the task specification (if any).
        #. The ``config`` section of the task specification (if any).

        The hyperparameters can be adopted by making changes to the returned object before running the task, but the
        changes are not reflected in the task specification.
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

    def resolve_path(self, path: Optional[PathLike]) -> Optional[pathlib.Path]:
        """
        Resolves a path relatively to the task directory.

        In addition, following placeholders are replaced:

        - ``{DIRNAME}``: The name of the task directory.
        - ``{ROOTDIR}``: The root directory of the task tree.
        """
        if path is None:
            return None

        # Replace placeholders in the path
        path = pathlib.Path(os.path.expanduser(str(path))
                            .replace('{DIRNAME}', self.path.name)
                            .replace('{ROOTDIR}', str(self.root.path.resolve())))

        # If the path is not absolute, treat it as relative to the task directory
        if not path.is_absolute():
            path = self.path / path

        # Resolve symlinks
        return path.resolve()

    def create_pipeline(self, *args, **kwargs) -> repype.pipeline.Pipeline:
        """
        Instantiates and returns the pipeline from the task specification.

        Can be overridden in subclasses to create custom pipelines.
        """
        pipeline = self.full_spec.get('pipeline')
        scopes = self.full_spec.get('scopes', dict())
        assert pipeline is not None
        assert isinstance(pipeline, (str, list))

        # Resolve scopes
        scopes = {key: self.resolve_path(value) for key, value in scopes.items()}

        # Load the pipeline from a module
        if isinstance(pipeline, str):
            pipeline_class = load_from_module(pipeline)
            return pipeline_class(*args, scopes = scopes, **kwargs)

        # Create the pipeline from a list of stages
        if isinstance(pipeline, list):
            stages = list()
            for stage in pipeline:
                stage_class = load_from_module(stage)
                stages.append(stage_class())
            return repype.pipeline.create_pipeline(stages, *args, scopes = scopes, **kwargs)

    def is_pending(self, pipeline: repype.pipeline.Pipeline, config: repype.config.Config) -> PendingReason:
        """
        Tells whether the task needs to run, or whether the task is completed or not runnable.

        Returns:
            - `"incomplete"`: The task needs to run because it is not completed.
            - `"pipeline"`: The task needs to run because the pipeline has changed.
            - `"specification"`: The task needs to run because the task specification or hyperparameters have changed.
            - `""`: The task is completed and does not need to run.
        """
        # Non-runnable tasks never are pending
        if not self.runnable:
            return ''

        # If the task is not completed, it is pending
        if not self.digest_sha_filepath.is_file():
            return 'incomplete'

        # Read the hashes of the completed task
        with self.digest_sha_filepath.open('r') as digest_sha_file:
            hashes = json.load(digest_sha_file)

        # If the task is completed, but the pipeline has changed, the task is pending
        for stage in pipeline.stages:
            if stage.sha != hashes['stages'][stage.id]:
                return 'pipeline'

        # If the task is completed, but the task specification or configuration have changed, the task is pending
        if hashes['task'] != self.compute_sha(config):
            return 'specification'
        else:
            return ''

    def reset(self):
        """
        Reset the task by removing all stored data.
        """
        if self.digest_sha_filepath.exists():
            self.digest_sha_filepath.unlink()
        if self.digest_task_filepath.exists():
            self.digest_task_filepath.unlink()
        if self.data_filepath.exists():
            self.data_filepath.unlink()

    def get_marginal_fields(self, pipeline: repype.pipeline.Pipeline) -> FrozenSet[str]:
        """
        Get the marginal fields from a pipeline.

        The marginal fields are all outputs produced by marginal stages. Marginal stages are those stages which are
        listed in the :attr:`marginal_stages` property. Marginal fields are removed from the *pipeline data objects*
        before merging it into the *task data object*, and when storing the results of the task.

        Args:
            pipeline: The pipeline object.

        Returns:
            Set of marginal fields.
        """
        marginal_fields = sum(
            (
                list(stage.outputs) for stage in pipeline.stages if stage.id in set(self.marginal_stages)
            ),
            list(),
        )
        return frozenset(marginal_fields)

    def load(self, pipeline: Optional[repype.pipeline.Pipeline] = None) -> TaskData:
        """
        Load the previously computed *task data object*.

        To ensure consistency with the task specification, it is verified that the loaded data contains results for all
        input identifiers of the task, and no spurious identifiers. If the `pipeline` is not `None`, a check for
        consistency of the data with the `pipeline` is also performed. The loaded *task data object* is consistent with
        the `pipeline` if the data contains all fields which are not marginal according to the
        :meth:`get_marginal_fields` method, and no additional fields.

        Args:
            pipeline: The pipeline object.

        Returns:
            The previously stored *task data object*.
        """
        assert self.runnable
        assert self.data_filepath.is_file()
        with gzip.open(self.data_filepath, 'rb') as data_file:
            data = dill.load(data_file)

        # Check if the data is consistent with the task specification
        assert (
            frozenset(data.keys()) == frozenset(self.input_ids)
        ), 'Loaded data is inconsistent with task specification.'

        # Check if the data is consistent with the pipeline
        if pipeline is not None:
            required_fields = pipeline.persistent_fields - self.get_marginal_fields(pipeline)
            assert all(
                (frozenset(data[input].keys()) == required_fields for input in data.keys())
            ), 'Loaded data is inconsistent with the pipeline.'

        # Return the loaded data
        return data

    def strip_marginals(self, pipeline: repype.pipeline.Pipeline, data_chunk: TaskData) -> PipelineData:
        """
        Strip the marginal fields from the *task data object*.

        Args:
            pipeline: The pipeline object.
            data_chunk: The *task data object* (not modified).

        Returns:
            Shallow copy of the *task data object* without the marginal fields.
        """
        marginal_fields = self.get_marginal_fields(pipeline)
        return {
            field: data_chunk[field] for field in data_chunk if field not in marginal_fields
        }

    def store(
            self,
            pipeline: repype.pipeline.Pipeline,
            data: TaskData,
            config: repype.config.Config,
            times: repype.benchmark.Benchmark[float],
        ) -> None:
        """
        Store the computed *task data object*.

        Arguments:
            pipeline: The pipeline used to compute `data`.
            data: The *task data object*.
            config: The hyperparameters used to compute `data`.
            times: The run times of the pipeline stages.
        """
        assert self.runnable
        assert frozenset(data.keys()) == frozenset(self.input_ids)

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

        # Store the run times of the pipeline stages
        assert (
            times.filepath == self.times_filepath
        ), f'Benchmark file path mismatch: "{times.filepath}" != "{self.times_filepath}"'
        times.retain((stage.id for stage in pipeline.stages), self.input_ids)
        times.save()

    def find_first_diverging_stage(
            self,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
        ) -> Optional[repype.stage.Stage]:
        """
        Find the first diverging stage of the task.

        Stages are considered diverging if they are new, or if their implementation or hyperparameters have changed.
        Changes of the implementation or hyperparameters of a stage are detected by comparing the SHA-1 hashes of the
        :meth:`stage.signature <repype.stage.Stage.signature>` and hyperparameters of the stage.

        Arguments:
            pipeline: The pipeline object.
            config: The hyperparameters.

        Returns:
            The first diverging stage of the task, or `None` if there is no diverging stage.
        """
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

            # Check if the stage configuration has changed  -- !! do not use `config.get` here to avoid mutation !!
            if self.digest['config'].get(stage.id, dict()) != config.entries.get(stage.id, dict()):
                return stage

        # There is no diverging stage
        return None

    def find_pickup_task(
            self,
            pipeline: repype.pipeline.Pipeline,
            config: repype.config.Config,
        ) -> Dict[str, Union[Self, repype.stage.Stage]]:
        """
        Find a previosly completed task to pick up computations from.

        Returns a dictionary with the following keys:

        - ``task``: The task to pick up from, or `None` if there is no task to pick up from.
        - ``first_diverging_stage``: The first stage of the `pipeline` which needs to run, or `None` if no further
          computations are required.

        Arguments:
            pipeline: The pipeline object.
            config: The hyperparameters.
        """
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
        ) -> TaskData:
        """
        Run the task.

        Arguments:
            config: The hyperparameters to run the task with.
            pipeline: The pipeline to run the task with. Defaults to :meth:`create_pipeline`.
            pickup: If `True`, pick up computations from a previously completed task.
            strip_marginals: If `True`, strip the marginal fields from the *task data object* before storing it.
            status: The status object to update.

        Raises:
            AssertionError: If the task is not runnable.
        """
        assert self.runnable
        if pipeline is None:
            pipeline = self.create_pipeline()
            self.setup_callbacks(pipeline)

        # Find a task and stage to pick up from
        if pickup:
            pickup_info = self.find_pickup_task(pipeline, config)
            if pickup_info['task'] is not None:
                data = pickup_info['task'].load(pipeline)
                times = self.times.set(pickup_info['task'].times)
                first_stage = pickup_info['first_diverging_stage']
            else:
                pickup = False

        # If there is no task to pick up from, run the pipeline from the beginning
        if not pickup:
            data = dict()
            times = repype.benchmark.Benchmark[float](self.times_filepath)
            first_stage = None

        # Announce the status of the task
        repype.status.update(
            status = status,
            info = 'start',
            task = str(self.path.resolve()),
            pickup = str(pickup_info['task'].path.resolve()) if pickup else None,
            first_stage = first_stage.id if first_stage else None,
        )

        # Skip pipeline run if `pickup` and `first_stage` is `None`
        if not pickup or first_stage is not None:

            # Run the pipeline for all inputs
            for input_idx, input_id in enumerate(self.input_ids):
                input_status = repype.status.derive(status)

                # Announce the status of the task
                repype.status.update(
                    status = input_status,
                    info = 'process',
                    task = str(self.path.resolve()),
                    input_id = input_id,
                    step = input_idx,
                    step_count = len(self.input_ids),
                )

                # Automatically adopt hyperparameters
                input_config = pipeline.configure(config.copy(), input_id)

                # Process the input
                data_chunk = data.get(input_id, dict())
                data_chunk, final_config, times_chunk = pipeline.process(
                    input_id = input_id,
                    data = data_chunk,
                    config = input_config,
                    first_stage = first_stage.id if first_stage else None,
                    status = input_status,
                )
                if strip_marginals:
                    data_chunk = self.strip_marginals(pipeline, data_chunk)

                # Update the times benchmark
                for stage_id, time in times_chunk.items():
                    times[stage_id, input_id] = time

                # Store the final configuration used for the input, if a corresponding scope is defined
                if final_config and (final_config_filepath := pipeline.resolve('config', input_id)):
                    final_config_filepath.parent.mkdir(parents = True, exist_ok = True)
                    with final_config_filepath.open('w') as final_config_file:
                        yaml.dump(final_config.entries, final_config_file)

                data[input_id] = data_chunk

        # Store the results for later pick up
        repype.status.update(status, info = 'storing', intermediate = True)
        self.store(pipeline, data, config, times)
        repype.status.update(
            status = status,
            info = 'completed',
            task = str(self.path.resolve()),
        )
        return data

    def __repr__(self):
        config = self.create_config()
        return f'<Task "{self.path}" {config.sha.hexdigest()[:7]}>'

    def setup_callbacks(self, pipeline: repype.pipeline.Pipeline) -> None:
        """
        Add callbacks to the pipeline stages.

        Callbacks are added to the pipeline stages for those stages and events, for which there is a corresponding
        method defined in the task object. The method name is constructed from the stage identifier and the event name,
        separated by underscores. For example, the method ``on_stage1_start`` is called when the stage with the
        identifier ``stage1`` starts.
        """
        for stage in pipeline.stages:
            for event in get_args(repype.stage.StageEvent):
                callback_name = f'on_{stage.id.replace("-", "_")}_{event}'
                if hasattr(self, callback_name):
                    stage.add_callback(event, getattr(self, callback_name))
