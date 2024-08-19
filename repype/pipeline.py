import builtins
import numbers
import pathlib

import repype.config
import repype.stage
import repype.status
from repype.typing import (
    Any,
    DataDictionary,
    Dict,
    FrozenSet,
    Input,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)


class ProcessingControl:
    """
    Class used to control the processing of stages in a pipeline.

    This class keeps track of the first and last stages of a pipeline,
    and determines whether a given stage should be processed based on its position in the pipeline.

    Arguments:
        first_stage: The first stage of the pipeline. Processing starts from this stage. If None, processing starts from the beginning.
        last_stage: The last stage of the pipeline. Processing ends after this stage. If None, processing goes until the end.
    """

    started: bool
    """
    Indicates whether processing has started (and not ended yet).
    """

    first_stage: Optional[str]
    """
    The first stage of the pipeline. Processing starts from this stage. If None, processing starts from the beginning.
    """

    last_stage: Optional[str]
    """
    The last stage of the pipeline. Processing ends after this stage. If None, processing goes until the end.
    """

    def __init__(self, first_stage: Optional[str] = None, last_stage: Optional[str] = None):
        self.started     = True if first_stage is None else False
        self.first_stage = first_stage
        self.last_stage  =  last_stage
    
    def step(self, stage: str) -> bool:
        """
        Determines whether the given stage should be processed.

        If the stage is the first stage of the pipeline, processing starts.
        If the stage is the last stage of the pipeline, processing ends after this stage.
        The attribute :attr:`started` is updated accordingly.

        Arguments:
            stage: The stage to check.
            
        Returns:
            True if the stage should be processed, False otherwise.
        """
        if not self.started and stage == self.first_stage:
            self.started = True
        do_step = self.started
        if stage == self.last_stage:
            self.started = False
        return do_step


def create_config_entry(
        config: repype.config.Config,
        key: str,
        factor: numbers.Real,
        default_user_factor: numbers.Real,
        type: Optional[Type[numbers.Real]] = None,
        min: Optional[numbers.Real] = None,
        max: Optional[numbers.Real] = None,
    ) -> None:
    """
    Creates the hyperparameter `key` in the `config` if it does not exist yet.

    The value of the hyperparameter is set to `factor` times the value of the hyperparameter ``AF_key``,
    where ``AF_key`` is the hyperparameter with the same key but prefixed with ``AF_``.

    In addition, the value of the hyperparameter is updated according to the following rules:

    - If `type` is not None, the value is converted to the specified type.
    - If `min` is not None, the value is set to the maximum of the value and `min`.
    - If `max` is not None, the value is set to the minimum of the value and `max`.

    See also:
        This function is used by the :py:meth:`Pipeline.configure` method to automatically configure the hyperparameters of the pipeline.
    """
    keys = key.split('/')
    af_key = f'{"/".join(keys[:-1])}/AF_{keys[-1]}'
    config.set_default(key, factor * config.get(af_key, default_user_factor), True)
    if type is not None: config.update(key, func=type)
    if  min is not None: config.update(key, func=lambda value: builtins.max((value, min)))
    if  max is not None: config.update(key, func=lambda value: builtins.min((value, max)))


class StageError(Exception):
    """
    An error raised when a stage fails to execute.
    """

    stage: repype.stage.Stage
    """
    The stage that failed to execute.
    """

    def __init__(self, stage: repype.stage.Stage):
        super().__init__(
            f'An error occured while executing the stage: {stage.id}'
        )
        self.stage = stage


class Pipeline:
    """
    Defines a processing pipeline.

    This class defines a processing pipeline that consists of multiple `stages`.
    Each stage performs a specific operation on the fields of the pipeline (i.e. the input data, or the data computed by a previous stage).
    The pipeline processes the input data by executing the :meth:`stage.process() <repype.stage.Stage.process>` method of each stage successively.

    In addition, the pipeline can be configured to use different `scopes` for resolving file paths using the :meth:`resolve` method
    (e.g., logs, debug information, or results to be written).
    """

    stages: List[repype.stage.Stage]
    """
    The stages of the pipeline.
    """

    scopes: Dict[str, pathlib.Path]
    """
    The scopes used to resolve file paths.
    """
    
    def __init__(
            self,
            stages: Iterable[repype.stage.Stage] = list(),
            scopes: Dict[str, pathlib.Path] = dict(),
        ):
        self.stages: List[repype.stage.Stage] = list(stages)
        self.scopes: Dict[str, pathlib.Path] = dict(scopes)

    def process(
            self,
            input: Optional[Input],
            config: repype.config.Config,
            first_stage: Optional[str] = None,
            last_stage: Optional[str] = None,
            data: Optional[DataDictionary] = None,
            status: Optional[repype.status.Status] = None,
            **kwargs,
        ) -> Tuple[DataDictionary, repype.config.Config, Dict[str, float]]:
        """
        Processes the input using this pipeline.

        The :py:meth:`stage.process() <repype.stage.Stage.process>` method of each stage of the pipeline is executed successively.

        Arguments:
            input: The input to be processed. Can be None if and only if `data` is not None (then the input is deduced from `data`).
            config: The hyperparameters to be used.
            first_stage: The ID of the first stage to be executed (or None to start with the first).
            last_stage: The ID of the last stage to be executed (or None to end with the last).
            data: The *pipeline data object* from a previous execution.
            status: A status object to report the progress of the computations.

        Returns:
            Tuple ``(data, config, timings)``, where ``data`` is the *pipeline data object* comprising all final and intermediate results, ``config`` are the finally used hyperparameters, and ``timings`` is a dictionary containing the execution time of each individual pipeline stage (in seconds).

        Raises:
            StageError: If an error occurs during the execution of a pipeline stage.

        The parameter `data` is required if and only if `first_stage` is not None, or `input` is None. In the former case, the outputs produced by the missing stages of the pipeline must be obtained from a previous execution of this method, and provided via the `data` parameter.
        """
        config = config.copy()

        # The canonical representation for starting the pipeline from the beginning is to set the `first_stage` to None
        if first_stage == self.stages[0].id:
            first_stage = None

        # If the `first_stage` ends with `+`, the pipeline is started from the next stage
        if first_stage is not None and first_stage.endswith('+'):
            first_stage = self.stages[1 + self.find(first_stage[:-1])].id

        # There is nothing to process if `first_stage` is after `last_stage`
        if first_stage is not None and last_stage is not None and self.find(first_stage) > self.find(last_stage):
            return data, config, dict()
        
        # The `data` parameter is required if `first_stage` is not None
        if first_stage is not None and first_stage != self.stages[0].id and data is None:
            raise ValueError('Argument "data" must be provided if "first_stage" is used')
        
        # The `input` parameter is required if `data` is not provided
        if data is None:
            data = dict()
        if input is not None:
            data['input'] = input

        # Determine the stages to be executed
        extra_stages = self.get_extra_stages(first_stage, last_stage, data.keys())
        ctrl = ProcessingControl(first_stage, last_stage)

        # Run the stages of the pipeline
        timings = {}
        for stage in self.stages:
            if ctrl.step(stage.id) or stage.id in extra_stages:
                stage_config = config.get(stage.id, {})
                try:
                    dt = stage.run(self, data, stage_config, status = status, **kwargs)
                except:
                    raise StageError(stage)
                timings[stage.id] = dt
            else:
                stage.skip(data, status = status, **kwargs)

        # Return the pipeline data object, the final config, and timings
        return data, config, timings
    
    def get_extra_stages(self, first_stage: Optional[str], last_stage: Optional[str], available_inputs: Iterable[str]) -> List[str]:
        """
        Returns the stages that are required to be executed in addition, in order to process the pipeline from `first_stage` to `last_stage`.

        Arguments:
            first_stage: The ID of the first stage to be executed (or None to start with the first).
            last_stage: The ID of the last stage to be executed (or None to end with the last).
            available_inputs: The fields that are already available (e.g., from previous computations).

        Returns:
            The IDs of the stages that are required to be executed in addition, in order to process the pipeline from `first_stage` to `last_stage`.
        """
        required_inputs, available_inputs = set(), set(available_inputs) | {'input'}
        stage_by_output = dict()
        extra_stages    = list()
        ctrl = ProcessingControl(first_stage, last_stage)
        for stage in self.stages:
            stage_by_output.update({output: stage for output in stage.outputs})
            if ctrl.step(stage.id):
                required_inputs  |= frozenset(stage.inputs)
                available_inputs |= frozenset(stage.outputs)
        while True:
            missing_inputs = required_inputs - available_inputs
            if len(missing_inputs) == 0: break
            extra_stage = stage_by_output[list(missing_inputs)[0]]
            required_inputs  |= frozenset(extra_stage.inputs)
            available_inputs |= frozenset(extra_stage.outputs)
            extra_stages.append(extra_stage.id)
        return extra_stages

    def find(self, stage_id: str, not_found_dummy: Any = float('inf')) -> repype.stage.Stage:
        """
        Returns the position of the stage identified by `stage_id`.

        Returns `not_found_dummy` if the stage is not found.
        """
        try:
            return [stage.id for stage in self.stages].index(stage_id)
        except ValueError:
            return not_found_dummy
        
    def stage(self, stage_id: str) -> Optional[repype.stage.Stage]:
        """
        Returns the stage identified by `stage_id`, or None if there is none.
        """
        idx = self.find(stage_id, None)
        return self.stages[idx] if idx is not None else None

    def append(self, stage: repype.stage.Stage, after: Optional[Union[str, int]] = None) -> int:
        """
        Adds a stage to the pipeline.

        By default, the stage is appended to the end of the pipeline.
        If `after` is given, the stage is instead inserted after the stage with the given ID or index.

        Returns:
            The index of the added stage.
        """
        for stage2 in self.stages:
            if stage2 is stage: raise RuntimeError(f'stage {stage.id} already added')
            if stage2.id == stage.id: raise RuntimeError(f'stage with ID {stage.id} already added')
        if after is None:
            self.stages.append(stage)
            return len(self.stages) - 1
        else:
            if isinstance(after, str): after = self.find(after)
            assert -1 <= after < len(self.stages)
            self.stages.insert(after + 1, stage)
            return after + 1

    def configure(self, base_config: repype.config.Config, *args, **kwargs) -> repype.config.Config:
        """
        Automatically configures hyperparameters.

        The hyperparameters are configured by the :py:func:`create_config_entry` function,
        and the arguments are determined by calling :py:meth:`stage.configure() <repype.stage.Stage.configure>` for each stage.
        """
        config = base_config.copy()
        for stage in self.stages:
            specs = stage.configure(*args, **kwargs)
            for key, spec in specs.items():
                assert len(spec) in (2,3), \
                    f'{type(stage).__name__}.configure returned tuple of unsupported length: {len(spec)}'
                create_config_entry_kwargs = dict() if len(spec) == 2 else spec[-1]
                create_config_entry(config, f'{stage.id}/{key}', *spec[:2], **create_config_entry_kwargs)
        return config
    
    def resolve(self, scope: str, input: Optional[Input] = None) -> Optional[pathlib.Path]:
        """
        Resolves the path of a file based on the given scope and input.

        Returns None if the input is None, or the scope is not defined.
        """
        if input is None or scope not in self.scopes:
            return None
        else:
            scope = self.scopes[scope]
            return pathlib.Path(str(scope) % input).resolve()
    
    @property
    def fields(self) -> FrozenSet[str]:
        """
        List all fields that are produced by the pipeline.
        """
        fields = set(['input'])
        for stage in self.stages:
            fields |= frozenset(stage.outputs)
        return frozenset(fields)
    
    @property
    def persistent_fields(self) -> FrozenSet[str]:
        """
        List all fields that are produced by the pipeline, minus those which are consumed.
        """
        fields = self.fields
        for stage in self.stages:
            fields -= frozenset(stage.consumes)
        return frozenset(fields)


def create_pipeline(stages: Sequence[repype.stage.Stage], *args, pipeline_cls: Type[Pipeline] = Pipeline, **kwargs) -> Pipeline:
    """
    Creates and returns a new pipeline configured for the given `stages`.

    Arguments:
        stages: The stages of the pipeline, the order is determined automatically.
        pipeline_cls: The class to be used for the pipeline.

    Returns:
        Object of the `pipeline_cls` class.
    """
    available_inputs = set(['input'])
    remaining_stages = list(stages)

    # Ensure that the stage identifiers are unique
    ids = [stage.id for stage in stages]
    assert len(ids) == len(frozenset(ids)), 'ambiguous stage identifiers'

    # Ensure that no output is produced more than once
    outputs = list(available_inputs) + sum((list(stage.outputs) for stage in stages), [])
    assert len(outputs) == len(frozenset(outputs)), 'ambiguous outputs'

    pipeline = pipeline_cls(*args, **kwargs)
    while len(remaining_stages) > 0:
        next_stage = None

        # Ensure that the next stage has no missing inputs
        for stage1 in remaining_stages:
            if frozenset(stage1.inputs).issubset(frozenset(available_inputs)):
                conflicted = False

                # Ensure that no remaining stage requires a consumed input
                for stage2 in remaining_stages:
                    if stage1 is stage2: continue
                    consumes = frozenset(getattr(stage1, 'consumes', []))
                    if len(consumes) > 0 and consumes.issubset(frozenset(stage2.inputs)):
                        conflicted = True

                if not conflicted:
                    next_stage = stage1
                    break

        if next_stage is None:
            raise RuntimeError(
                f'Failed to resolve total ordering (pipeline so far: {pipeline.stages}, '
                f'available inputs: {available_inputs}, remaining stages: {remaining_stages})')
        
        remaining_stages.remove(next_stage)
        pipeline.append(next_stage)
        available_inputs |= frozenset(getattr(next_stage, 'outputs' , []))
        available_inputs -= frozenset(getattr(next_stage, 'consumes', []))

    return pipeline