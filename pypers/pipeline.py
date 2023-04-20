import time
import weakref
import os
import re

from typing import Union
from collections.abc import Sequence

from .config import Config
from .output import get_output


def suggest_cfgns(class_name):
    assert class_name != '_' and re.match('[a-zA-Z]', class_name) and re.match('^[a-zA-Z_](?:[a-zA-Z0-9_])*$', class_name), f'not a valid class name: "{class_name}"'
    tokens1 = re.findall('[A-Z0-9][^A-Z0-9_]*', class_name)
    tokens2 = list()
    i1 = 0
    while i1 < len(tokens1):
        token = tokens1[i1]
        i1 += 1
        if len(token) == 1:
            for t in tokens1[i1:]:
                if len(t) == 1 and (token.isnumeric() == t.isnumeric() or token.isalpha() == t.isalpha()):
                    token += t
                    i1 += 1
                else:
                    break
        tokens2.append(token.lower().replace('_', ''))
    if len(tokens2) >= 2 and tokens2[-1] == 'stage': tokens2 = tokens2[:-1]
    return '-'.join(tokens2)


class Stage(object):
    """A pipeline stage.

    Each stage can be controlled by a separate set of hyperparameters. Refer to the documentation of the respective pipeline stages for details. Most hyperparameters reside in namespaces, which are uniquely associated with the corresponding pipeline stages.

    :param name: Readable identifier of this stage.
    :param cfgns: Hyperparameter namespace of this stage. Defaults to ``name`` if not specified.
    :param inputs: List of inputs required by this stage.
    :param outputs: List of outputs produced by this stage.

    Automation
    ^^^^^^^^^^

    Hyperparameters can be set automatically using the :py:meth:`~.configure` method based on the scale :math:`\sigma` of objects in an image. Hyperparameters are only set automatically based on the scale of objects, if the :py:mod:`~superdsm.automation` module (as in :ref:`this <usage_example_interactive>` example) or batch processing are used (as in :ref:`this <usage_example_batch>` example). Hyperparameters are *not* set automatically if the :py:meth:`~superdsm.pipeline.Pipeline.process_image` method of the :py:class:`~superdsm.pipeline.Pipeline` class is used directly.

    Inputs and outputs
    ^^^^^^^^^^^^^^^^^^

    Each stage must declare its required inputs and the outputs it produces. These are used by :py:meth:`~.create_pipeline` to automatically determine the stage order. The input ``g_raw`` is provided by the pipeline itself.
    """

    inputs   = []
    outputs  = []
    consumes = []
    enabled_by_default = True

    def __init__(self):
        self.cfgns    = type(self).cfgns if hasattr(type(self), 'cfgns') else suggest_cfgns(type(self).__name__)
        self.inputs   = frozenset(type(self).inputs) | frozenset(type(self).consumes)
        self.outputs  = frozenset(type(self).outputs)
        self.consumes = frozenset(type(self).consumes)
        self.enabled_by_default = type(self).enabled_by_default
        assert not self.cfgns.endswith('+'), 'the suffix "+" is reserved as an indication of "the stage after that stage"'
        self._callbacks = {}

    def _callback(self, name, *args, **kwargs):
        if name in self._callbacks:
            for cb in self._callbacks[name]:
                cb(self, name, *args, **kwargs)

    def add_callback(self, name, cb):
        if name == 'after':
            self.add_callback( 'end', cb)
            self.add_callback('skip', cb)
        else:
            if name not in self._callbacks: self._callbacks[name] = []
            self._callbacks[name].append(cb)

    def remove_callback(self, name, cb):
        if name == 'after':
            self.remove_callback( 'end', cb)
            self.remove_callback('skip', cb)
        else:
            if name in self._callbacks: self._callbacks[name].remove(cb)

    def __call__(self, data, cfg, out=None, log_root_dir=None, **kwargs):
        out = get_output(out)
        cfg = cfg.get(self.cfgns, {})
        if cfg.get('enabled', self.enabled_by_default):
            out.intermediate(f'Starting stage "{self.cfgns}"')
            self._callback('start', data, **kwargs)
            input_data = {key: data[key] for key in self.inputs}
            clean_cfg = cfg.copy()
            clean_cfg.pop('enabled', None)
            t0 = time.time()
            output_data = self.process(cfg=clean_cfg, log_root_dir=log_root_dir, out=out, **input_data)
            dt = time.time() - t0
            assert len(set(output_data.keys()) ^ set(self.outputs)) == 0, 'stage "%s" produced spurious or missing output' % self.cfgns
            data.update(output_data)
            for key in self.consumes: del data[key]
            self._callback('end', data, **kwargs)
            return dt
        else:
            out.write(f'Skipping disabled stage "{self.cfgns}"')
            self._callback('skip', data, **kwargs)
            return 0
        
    def skip(self, data, **kwargs):
            self._callback('skip', data, **kwargs)

    def process(self, cfg, log_root_dir, out, **inputs):
        """Runs this pipeline stage.

        :param input_data: Dictionary of the inputs declared by this stage.
        :param cfg: The hyperparameters to be used by this stage.
        :param log_root_dir: Path of directory where log files will be written, or ``None`` if no log files should be written.
        :param out: An instance of an :py:class:`~superdsm.output.Output` sub-class, ``'muted'`` if no output should be produced, or ``None`` if the default output should be used.
        :return: Dictionary of the outputs declared by this stage.
        """
        raise NotImplementedError()

    def configure(self, *args, **kwargs):
        """Automatically computes the default configuration entries which are dependent on the scale of the objects in an image.

        :return: Dictionary of configuration entries of the form:

            .. code-block:: python

               {
                   'key': (factor, default_user_factor),
               }
            
            Each hyperparameter ``key`` is associated with a new hyperparameter ``AF_key``. The value of the hyperparameter ``key`` will be computed as the product of ``factor`` and the value of the ``AF_key`` hyperparameter, which defaults to ``default_user_factor``. Another dictionary may be provided as a third component of the tuple, which can specify a ``type``, ``min``, and ``max`` values.
        """
        return dict()

    def __str__(self):
        return self.cfgns

    def __repr__(self):
        return f'<{type(self).__name__}, cfgns: {self.cfgns}>'


class ProcessingControl:

    def __init__(self, first_stage=None, last_stage=None):
        self.started     = True if first_stage is None else False
        self.first_stage = first_stage
        self.last_stage  =  last_stage
    
    def step(self, stage):
        if not self.started and stage == self.first_stage: self.started = True
        do_step = self.started
        if stage == self.last_stage: self.started = False
        return do_step


def _create_config_entry(cfg, key, factor, default_user_factor, type=None, min=None, max=None):
    keys = key.split('/')
    af_key = f'{"/".join(keys[:-1])}/AF_{keys[-1]}'
    cfg.set_default(key, factor * cfg.get(af_key, default_user_factor), True)
    if type is not None: cfg.update(key, func=type)
    if  min is not None: cfg.update(key, func=lambda value: _max((value, min)))
    if  max is not None: cfg.update(key, func=lambda value: _min((value, max)))


class Configurator:
    """Automatically configures hyperparameters of a pipeline.
    """

    def __init__(self, pipeline: 'Pipeline'):
        assert pipeline is not None
        self._pipeline = weakref.ref(pipeline)

    @property
    def pipeline(self):
        pipeline = self._pipeline()
        assert pipeline is not None
        return pipeline
    
    def configure(self, base_cfg, input):
        return self.pipeline.configure(base_cfg, input)
    
    def first_differing_stage(self, config1: 'Config', config2: 'Config'):
        for stage in self.pipeline.stages:
            if any([
                stage.cfgns in config1 and stage.cfgns not in config2,
                stage.cfgns not in config1 and stage.cfgns in config2,
                stage.cfgns in config1 and stage.cfgns in config2 and config1[stage.cfgns] != config2[stage.cfgns],
            ]):
                return stage
        return None


class Pipeline:
    """Represents a processing pipeline.
    
    Note that hyperparameters are *not* set automatically if the :py:meth:`~.process_image` method is used directly. Hyperparameters are only set automatically, if the :py:mod:`~.configure` method or batch processing are used.
    """
    
    def __init__(self, configurator: 'Configurator' = None):
        self.stages = []
        self.configurator = configurator if configurator else Configurator(self)

    def process(self, input, cfg, first_stage=None, last_stage=None, data=None, log_root_dir=None, out=None, **kwargs):
        """Processes the input.

        The :py:meth:`~.Stage.process` methods of the stages of the pipeline are executed successively.

        :param input: The input to be processed (can be ``None`` if and only if ``data`` is not ``None``).
        :param cfg: A :py:class:`~superdsm.config.Config` object which represents the hyperparameters.
        :param first_stage: The name of the first stage to be executed.
        :param last_stage: The name of the last stage to be executed.
        :param data: The results of a previous execution.
        :param log_root_dir: Path to a directory where log files should be written to.
        :param out: An instance of an :py:class:`~superdsm.output.Output` sub-class, ``'muted'`` if no output should be produced, or ``None`` if the default output should be used.
        :return: Tuple ``(data, cfg, timings)``, where ``data`` is the *pipeline data object* comprising all final and intermediate results, ``cfg`` are the finally used hyperparameters, and ``timings`` is a dictionary containing the execution time of each individual pipeline stage (in seconds).

        The parameter ``data`` is used if and only if ``first_stage`` is not ``None``. In this case, the outputs produced by the stages of the pipeline which are being skipped must be fed in using the ``data`` parameter obtained from a previous execution of this method.
        """
        cfg = cfg.copy()
        if log_root_dir is not None: os.makedirs(log_root_dir, exist_ok=True)
        if first_stage == self.stages[0].cfgns and data is None: first_stage = None
        if first_stage is not None and first_stage.endswith('+'): first_stage = self.stages[1 + self.find(first_stage[:-1])].cfgns
        if first_stage is not None and last_stage is not None and self.find(first_stage) > self.find(last_stage): return data, cfg, {}
        if first_stage is not None and first_stage != self.stages[0].cfgns and data is None: raise ValueError('data argument must be provided if first_stage is used')
        if data is None: data = dict()
        if input is not None: data['input'] = input
        extra_stages = self.get_extra_stages(first_stage, last_stage, data.keys())
        out  = get_output(out)
        ctrl = ProcessingControl(first_stage, last_stage)
        timings = {}
        for stage in self.stages:
            if ctrl.step(stage.cfgns) or stage.cfgns in extra_stages:
                dt = stage(data, cfg, out=out, log_root_dir=log_root_dir, **kwargs)
                timings[stage.cfgns] = dt
            else:
                stage.skip(data, **kwargs)
        return data, cfg, timings
    
    def get_extra_stages(self, first_stage, last_stage, available_inputs):
        required_inputs, available_inputs = set(), set(available_inputs) | {'input'}
        stage_by_output = dict()
        extra_stages    = list()
        ctrl = ProcessingControl(first_stage, last_stage)
        for stage in self.stages:
            stage_by_output.update({output: stage for output in stage.outputs})
            if ctrl.step(stage.cfgns):
                required_inputs  |= stage.inputs
                available_inputs |= stage.outputs
        while True:
            missing_inputs = required_inputs - available_inputs
            if len(missing_inputs) == 0: break
            extra_stage = stage_by_output[list(missing_inputs)[0]]
            required_inputs  |= extra_stage.inputs
            available_inputs |= extra_stage.outputs
            extra_stages.append(extra_stage.cfgns)
        return extra_stages

    def find(self, cfgns, not_found_dummy=float('inf')):
        """Returns the position of the stage identified by ``stage_cfgns``.

        Returns ``not_found_dummy`` if the stage is not found.
        """
        try:
            return [stage.cfgns for stage in self.stages].index(cfgns)
        except ValueError:
            return not_found_dummy
        
    def stage(self, cfgns):
        idx = self.find(cfgns, None)
        return self.stages[idx] if idx is not None else None

    def append(self, stage: 'Stage', after: Union[str, int] = None):
        for stage2 in self.stages:
            if stage2 is stage: raise RuntimeError(f'stage {stage.cfgns} already added')
            if stage2.cfgns == stage.cfgns: raise RuntimeError(f'stage with namespace {stage.cfgns} already added')
        if after is None:
            self.stages.append(stage)
            return len(self.stages) - 1
        else:
            if isinstance(after, str): after = self.find(after)
            assert -1 <= after < len(self.stages)
            self.stages.insert(after + 1, stage)
            return after + 1

    def configure(self, base_cfg, *args, **kwargs):
        """Automatically configures hyperparameters.
        """
        cfg = base_cfg.copy()
        for stage in self.stages:
            specs = stage.configure(*args, **kwargs)
            for key, spec in specs.items():
                assert len(spec) in (2,3), f'{type(stage).__name__}.configure returned tuple of unknown length ({len(spec)})'
                _create_config_entry_kwargs = dict() if len(spec) == 2 else spec[-1]
                _create_config_entry(cfg, f'{stage.cfgns}/{key}', *spec[:2], **_create_config_entry_kwargs)
        return cfg
    
    @property
    def fields(self):
        fields = set(['input'])
        for stage in self.stages:
            fields |= stage.outputs
        return fields


def create_pipeline(stages: Sequence):
    """Creates and returns a new :py:class:`.Pipeline` object configured for the given stages.

    The stage order is determined automatically.
    """
    available_inputs = set(['input'])
    remaining_stages = list(stages)

    # Ensure that the stage namespaces are unique
    namespaces = [stage.cfgns for stage in stages]
    assert len(namespaces) == len(frozenset(namespaces)), 'ambiguous namespaces'

    # Ensure that no output is produced more than once
    outputs = list(available_inputs) + sum((list(stage.outputs) for stage in stages), [])
    assert len(outputs) == len(frozenset(outputs)), 'ambiguous outputs'

    pipeline = Pipeline()
    while len(remaining_stages) > 0:
        next_stage = None

        # Ensure that the next stage has no missing inputs
        for stage1 in remaining_stages:
            if stage1.inputs.issubset(available_inputs):
                conflicted = False

                # Ensure that no remaining stage requires a consumed input
                for stage2 in remaining_stages:
                    if stage1 is stage2: continue
                    if len(stage1.consumes) > 0 and stage1.consumes.issubset(stage2.inputs):
                        conflicted = True

                if not conflicted:
                    next_stage = stage1
                    break

        if next_stage is None:
            raise RuntimeError(f'failed to resolve total ordering (pipeline so far: {pipeline.stages}, available inputs: {available_inputs}, remaining stages: {remaining_stages})')
        remaining_stages.remove(next_stage)
        pipeline.append(next_stage)
        available_inputs |= next_stage.outputs
        available_inputs -= next_stage.consumes

    return pipeline

