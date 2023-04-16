import time

from .output import get_output


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

    def __init__(self, name: str, cfgns: str = None, inputs: list = [], outputs: list = [], consumes: list = [], enabled_by_default: bool = True):
        if cfgns is None: cfgns = name
        self.name     = name
        self.cfgns    = cfgns
        self.inputs   = frozenset(inputs) | frozenset(consumes)
        self.outputs  = frozenset(outputs)
        self.consumes = frozenset(consumes)
        self.enabled_by_default = enabled_by_default
        self._callbacks = {}

    def _callback(self, name, *args, **kwargs):
        if name in self._callbacks:
            for cb in self._callbacks[name]:
                cb(name, *args, **kwargs)

    def add_callback(self, name, cb):
        if name not in self._callbacks: self._callbacks[name] = []
        self._callbacks[name].append(cb)

    def remove_callback(self, name, cb):
        if name in self._callbacks: self._callbacks[name].remove(cb)

    def __call__(self, data, cfg, out=None, log_root_dir=None):
        out = get_output(out)
        cfg = cfg.get(self.cfgns, {})
        if cfg.get('enabled', self.enabled_by_default):
            out.intermediate(f'Starting stage "{self.name}"')
            self._callback('start', data)
            input_data = {key: data[key] for key in self.inputs}
            t0 = time.time()
            output_data = self.process(input_data, cfg=cfg, log_root_dir=log_root_dir, out=out)
            dt = time.time() - t0
            assert len(set(output_data.keys()) ^ set(self.outputs)) == 0, 'stage "%s" generated unexpected output' % self.name
            data.update(output_data)
            for key in self.consumes: del data[key]
            self._callback('end', data)
            return dt
        else:
            out.write(f'Skipping disabled stage "{self.name}"')
            self._callback('skip', data)
            return 0

    def process(self, input_data, cfg, log_root_dir, out):
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
            
            Each hyperparameter ``key`` is associated with a new hyperparameter ``AF_key``. The value of the hyperparameter ``key`` will be computed as the product of ``factor`` and the value of the ``AF_key`` hyperparameter, which defaults to ``default_user_factor``. The value given for ``factor`` is usually ``scale``, ``radius``, ``diameter``, or a polynomial thereof. Another dictionary may be provided as a third component of the tuple, which can specify a ``type``, ``min``, and ``max`` values.
        """
        return dict()


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


class Pipeline:
    """Represents a processing pipeline.
    
    Note that hyperparameters are *not* set automatically if the :py:meth:`~.process_image` method is used directly. Hyperparameters are only set automatically, if the :py:mod:`~.configure` method or batch processing are used.
    """
    
    def __init__(self):
        self.stages = []

    def process(self, input, cfg, first_stage=None, last_stage=None, data=None, log_root_dir=None, out=None):
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
        if log_root_dir is not None: mkdir(log_root_dir)
        if first_stage == self.stages[0].name and data is None: first_stage = None
        if first_stage is not None and first_stage.endswith('+'): first_stage = self.stages[1 + self.find(first_stage[:-1])].name
        if first_stage is not None and last_stage is not None and self.find(first_stage) > self.find(last_stage): return data, cfg, {}
        if first_stage is not None and first_stage != self.stages[0].name and data is None: raise ValueError('data argument must be provided if first_stage is used')
        out  = get_output(out)
        ctrl = ProcessingControl(first_stage, last_stage)
        if data is None: data = dict(input=input)
        timings = {}
        for stage in self.stages:
            if ctrl.step(stage.name):
                dt = stage(data, cfg, out=out, log_root_dir=log_root_dir)
                timings[stage.name] = dt
        return data, cfg, timings

    def find(self, stage_name, not_found_dummy=float('inf')):
        """Returns the position of the stage identified by ``stage_name``.

        Returns ``not_found_dummy`` if the stage is not found.
        """
        try:
            return [stage.name for stage in self.stages].index(stage_name)
        except ValueError:
            return not_found_dummy

    def append(self, stage, after=None):
        if after is None: self.stages.append(stage)
        else:
            if isinstance(after, str): after = self.find(after)
            self.stages.insert(after + 1, stage)

    def configure(self, base_cfg, *args, **kwargs):
        """Automatically configures hyperparameters based on the scale of objects in an image.
        """
        cfg = base_cfg.copy()
        for stage in self.stages:
            specs = stage.configure(*args, **kwargs)
            for key, spec in specs.items():
                assert len(spec) in (2,3), f'{type(stage).__name__}.configure returned tuple of unknown length ({len(spec)})'
                kwargs = dict() if len(spec) == 2 else spec[-1]
                _create_config_entry(cfg, f'{stage.cfgns}/{key}', *spec[:2], **kwargs)
        return cfg


def create_pipeline(stages):
    """Creates and returns a new :py:class:`.Pipeline` object configured for the given stages.

    The stage order is determined automatically.
    """
    available_inputs = set(['input'])
    remaining_stages = list(stages)

    pipeline = Pipeline()
    while len(remaining_stages) > 0:
        next_stage = None

        # Ensure that the next stage has no missing inputs
        for stage1 in remaining_stages:
            if stage1.inputs.issubset(available_inputs):
                conflicted = False

                # Ensure that no remaining stage requires a consumed input
                for stage2 in remaining_stages - {stage1}:
                    if stage1.consumes.issubset(stage2.inputs):
                        conflicted = True

                if not conflicted:
                    next_stage = stage
                    break

        if next_stage is None:
            raise ValueError('failed to resolve total ordering')
        remaining_stages.remove(next_stage)
        pipeline.append(next_stage)
        available_inputs |= next_stage.outputs
        available_inputs -= next_stage.consumes

    return pipeline

