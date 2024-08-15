import builtins
import os
import pathlib

import repype.config
import repype.stage
import repype.status
from repype.typing import (
    Any,
    Dict,
    Input,
    Iterable,
    List,
    Optional,
    Sequence,
    Type,
    Union,
)


class ProcessingControl:
    """
    A class used to control the processing of stages in a pipeline.

    This class keeps track of the first and last stages of a pipeline, and determines whether a given stage should be processed based on its position in the pipeline.

    :param first_stage: The first stage of the pipeline. Processing starts from this stage. If None, processing starts from the beginning.
    :type first_stage: str, optional
    :param last_stage: The last stage of the pipeline. Processing stops after this stage. If None, processing goes until the end.
    :type last_stage: str, optional
    """

    def __init__(self, first_stage: Optional[str]=None, last_stage: Optional[str]=None):
        self.started     = True if first_stage is None else False
        self.first_stage = first_stage
        self.last_stage  =  last_stage
    
    def step(self, stage):
        """
        Determines whether the given stage should be processed.

        If the stage is the first stage of the pipeline, processing starts. If the stage is the last stage of the pipeline, processing stops after this stage.

        :param stage: The stage to check.
        :type stage: str
        :return: True if the stage should be processed, False otherwise.
        :rtype: bool
        """
        if not self.started and stage == self.first_stage: self.started = True
        do_step = self.started
        if stage == self.last_stage: self.started = False
        return do_step


def create_config_entry(cfg, key, factor, default_user_factor, type=None, min=None, max=None):
    keys = key.split('/')
    af_key = f'{"/".join(keys[:-1])}/AF_{keys[-1]}'
    cfg.set_default(key, factor * cfg.get(af_key, default_user_factor), True)
    if type is not None: cfg.update(key, func=type)
    if  min is not None: cfg.update(key, func=lambda value: builtins.max((value, min)))
    if  max is not None: cfg.update(key, func=lambda value: builtins.min((value, max)))


class Pipeline:
    """
    Defines a processing pipeline.

    This class defines a processing pipeline that consists of multiple stages. Each stage performs a specific operation on the input data. The pipeline processes the input data by executing the `process` method of each stage successively.

    Note that hyperparameters are *not* set automatically if the :py:meth:`~.process_image` method is used directly. Hyperparameters are only set automatically if the :py:mod:`~.configure` method or batch processing is used.
    """
    
    def __init__(self, stages: Iterable[repype.stage.Stage] = list()):
        self.stages: List[repype.stage.Stage] = list(stages)
        self.scopes: Dict[str, pathlib.Path] = dict()

    def process(self, input, config, first_stage=None, last_stage=None, data=None, log_root_dir=None, status=None, **kwargs):
        """
        Processes the input.

        The :py:meth:`~.Stage.process` methods of the stages of the pipeline are executed successively.

        :param input: The input to be processed (can be ``None`` if and only if ``data`` is not ``None``).
        :param config: A :py:class:`~repype.config.Config` object that represents the hyperparameters.
        :param first_stage: The name of the first stage to be executed.
        :param last_stage: The name of the last stage to be executed.
        :param data: The results of a previous execution.
        :param log_root_dir: Path to a directory where log files should be written to.
        :param status: A :py:class:`~repype.status.Status` object.
        :return: Tuple ``(data, cfg, timings)``, where ``data`` is the *pipeline data object* comprising all final and intermediate results, ``cfg`` are the finally used hyperparameters, and ``timings`` is a dictionary containing the execution time of each individual pipeline stage (in seconds).

        The parameter ``data`` is used if and only if ``first_stage`` is not ``None``. In this case, the outputs produced by the stages of the pipeline which are being skipped must be fed in using the ``data`` parameter obtained from a previous execution of this method.
        """
        config = config.copy()
        if log_root_dir is not None: os.makedirs(log_root_dir, exist_ok=True)
        if first_stage == self.stages[0].id and data is None: first_stage = None
        if first_stage is not None and first_stage.endswith('+'): first_stage = self.stages[1 + self.find(first_stage[:-1])].id
        if first_stage is not None and last_stage is not None and self.find(first_stage) > self.find(last_stage): return data, config, {}
        if first_stage is not None and first_stage != self.stages[0].id and data is None: raise ValueError('data argument must be provided if first_stage is used')
        if data is None: data = dict()
        if input is not None: data['input'] = input
        extra_stages = self.get_extra_stages(first_stage, last_stage, data.keys())
        ctrl = ProcessingControl(first_stage, last_stage)
        timings = {}
        for stage in self.stages:
            if ctrl.step(stage.id) or stage.id in extra_stages:
                stage_config = config.get(stage.id, {})
                try:
                    dt = stage(self, data, stage_config, status = repype.status.derive(status), log_root_dir = log_root_dir, **kwargs)
                except:
                    print(f'An error occured while executing the stage: {str(stage)}')
                    raise
                timings[stage.id] = dt
            else:
                stage.skip(data, status = status, **kwargs)
        return data, config, timings
    
    def get_extra_stages(self, first_stage, last_stage, available_inputs):
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
        Returns the position of the stage identified by ``stage_id``.

        Returns ``not_found_dummy`` if the stage is not found.
        """
        try:
            return [stage.id for stage in self.stages].index(stage_id)
        except ValueError:
            return not_found_dummy
        
    def stage(self, stage_id):
        idx = self.find(stage_id, None)
        return self.stages[idx] if idx is not None else None

    def append(self, stage: repype.stage.Stage, after: Union[str, int] = None):
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
    
    def resolve(self, scope: str, input: Optional[Input] = None) -> pathlib.Path:
        """
        Resolves the path of a file based on the given scope and input.
        """
        if input is None:
            return None
        else:
            scope = self.scopes[scope]
            return pathlib.Path(str(scope) % input).resolve()
    
    @property
    def fields(self):
        fields = set(['input'])
        for stage in self.stages:
            fields |= frozenset(stage.outputs)
        return frozenset(fields)


def create_pipeline(stages: Sequence[repype.stage.Stage], *args, pipeline_cls: Type[Pipeline] = Pipeline, **kwargs) -> Pipeline:
    """
    Creates and returns a new :py:class:`.Pipeline` object configured for the given stages.

    The stage order is determined automatically.
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