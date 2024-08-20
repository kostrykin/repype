import hashlib
import json
import re
import time

import repype.config
import repype.status
from repype.typing import (
    Collection,
    PipelineData,
    Dict,
    Optional,
    Pipeline,
    Protocol,
    Input,
    Iterable,
    List,
    Literal,
)


StageEvent = Literal[
    'start',
    'end',
    'skip',
    'after',
]
class StageCallback(Protocol):
    """
    Stage callback protocol.
    """

    def __call__(self, stage: 'Stage', event: StageEvent, *args, **kwargs) -> None:
        """
        Arguments:
            stage: The stage that triggered the event.
            event: The event that triggered the callback.
            *args: The arguments passed to the callback.
            **kwargs: The keyword arguments passed to the callback.
        """
        pass


def suggest_stage_id(class_name: str) -> str:
    """
    Suggest stage ID based on a class name.

    This function validates the class name, then finds and groups tokens in the class name.
    Tokens are grouped if they are consecutive and alphanumeric, but do not start with numbers.
    The function then converts the tokens to lowercase, removes underscores, and joins them with hyphens.

    Arguments:
        class_name: The name of the class to suggest a configuration namespace for.

    Returns:
        A string of hyphen-separated tokens from the class name.
    
    Raises:
        AssertionError: If the class name is not valid.
    """
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


class Stage:
    """
    A pipeline stage.

    Each stage can be controlled by a separate set of hyperparameters.
    Those hyperparameters reside in namespaces, which are uniquely associated with the corresponding pipeline stages.

    Each stage must declare the input fields it requires, and the output fields it produces.
    These are used by :func:`repype.pipeline.create_pipeline` function to automatically determine the stage order and
    by the :meth:`repype.pipeline.Pipeline.get_extra_stages` method to determine the stages that are required to be executed additionally.
    The input field ``input`` is provided by the pipeline itself via the :meth:`repype.pipeline.Pipeline.process` method.

    Arguments:
        id: The stage ID, used as the hyperparameter namespace.
            Defaults to the result of the :py:func:`suggest_stage_id` function.
        inputs: List of inputs required by this stage.
        consumes: List of inputs consumed by this stage (cannot be used by subsequent stages).
        outputs: List of outputs produced by this stage.
    """

    inputs: Collection[str] = []
    """
    List of inputs required by this stage.
    """

    outputs: Collection[str] = []
    """
    List of outputs produced by this stage.
    """

    consumes: Collection[str] = []
    """
    List of inputs consumed by this stage (cannot be used by subsequent stages).
    """

    enabled_by_default: bool = True
    """
    Whether the stage is enabled by default.

    The default value can be overridden by the ``enabled`` hyperparameter of the stage.
    """

    def __init__(self):
        self.id       = type(self).id if hasattr(type(self), 'id') else suggest_stage_id(type(self).__name__)
        self.inputs   = frozenset(type(self).inputs) | frozenset(type(self).consumes)
        self.outputs  = frozenset(type(self).outputs)
        self.consumes = frozenset(type(self).consumes)
        self.enabled_by_default = type(self).enabled_by_default
        assert not self.id.endswith('+'), 'The suffix "+" is reserved as an indication of "the stage after that stage"'
        self.event_callbacks: Dict[StageEvent, List[StageCallback]] = dict()

    def callback(self, event: StageEvent, *args, **kwargs) -> None:
        """
        Call the callbacks for the specified `event`.

        Arguments:
            event: The event for which to call the callbacks.
            *args: The arguments to pass to the callbacks.
            **kwargs: The keyword arguments to pass to the callbacks.
        """
        if event in self.event_callbacks:
            for callback in self.event_callbacks[event]:
                callback(self, event, *args, **kwargs)

    def add_callback(self, event: StageEvent, callback: StageCallback) -> None:
        """
        Add a callback for the specified `event`.
        """
        if event == 'after':
            self.add_callback( 'end', callback)
            self.add_callback('skip', callback)
        else:
            if event not in self.event_callbacks:
                self.event_callbacks[event] = list()
            self.event_callbacks[event].append(callback)

    def remove_callback(self, event: StageEvent, callback: StageCallback) -> None:
        """
        Remove a callback for the specified `event`.
        """
        if event == 'after':
            self.remove_callback( 'end', callback)
            self.remove_callback('skip', callback)
        else:
            if event in self.event_callbacks:
                self.event_callbacks[event].remove(callback)

    def run(
            self,
            pipeline: Pipeline,
            data: PipelineData,
            config: repype.config.Config,
            status: Optional[repype.status.Status] = None,
            **kwargs,
        ) -> float:
        """
        Run this stage of the `pipeline` by calling :meth:`process`, if the stage is enabled.

        The stage is enabled if the ``enabled`` hyperparameter is set to ``True``, or
        the ``enabled`` hyperparameter is not set and :attr:`enabled_by_default` is True.

        Arguments:
            pipeline: The pipeline object that this stage is a part of.
            data: The *pipeline data object* to be used for this stage.
                This is a dictionary that contains all required inputs.
                The outputs of this stage are added to this dictionary.
            config: The hyperparameters to be used for this stage.
            status: A status object to report the progress of the computations.

        Returns:
            The duration of the stage run in seconds, if the stage is enabled,
            and 0 otherwise.
        """

        # Run the stage if it is enabled
        if config.get('enabled', self.enabled_by_default):
            repype.status.update(
                status = status,
                info = 'start-stage',
                stage = self.id,
                intermediate = True,
            )
            self.callback('start', data, status = status, config = config, **kwargs)

            # Extract the input data of the stage
            input_data = {key: data[key] for key in self.inputs}

            # Clean up the hyperparameters passed to the stage implementation
            clean_config = config.copy()
            clean_config.pop('enabled', None)

            # Run the stage and measure the run time
            t0 = time.time()
            output_data = self.process(
                pipeline = pipeline,
                config = clean_config,
                status = status,
                **input_data,
            )
            dt = time.time() - t0

            # Check the output data produced by the stage
            assert len(set(output_data.keys()) ^ set(self.outputs)) == 0, f'Stage "{self.id}" produced spurious or missing output'
            data.update(output_data)
            for key in self.consumes:
                del data[key]

            # Finish the stage
            self.callback('end', data, status = status, config = config, **kwargs)
            return dt
        
        # Skip the stage
        else:
            self.skip(data, status = status, config = config, **kwargs)
            return 0.
        
    def skip(self, data: PipelineData, status: Optional[repype.status.Status] = None, **kwargs) -> None:
        """
        Skips this stage of the pipeline.

        Arguments:
            data: The *pipeline data object* to be used for this stage.
                This is a dictionary that contains all required inputs.
            status: A status object to report the progress of the computations.
        """
        repype.status.update(
            status = status,
            info = 'skip-stage',
            stage = self.id,
            intermediate = True,
        )
        self.callback('skip', data, status = status, **kwargs)

    def process(
            self,
            pipeline: Pipeline,
            config: repype.config.Config,
            status: Optional[repype.status.Status] = None,
            **inputs,
        ) -> PipelineData:
        """
        Processes the inputs of this stage of the `pipeline`.

        This method implements a stage of the pipeline with the provided inputs and configuration parameters.
        It then returns the outputs produced by this stage.

        Arguments:
            pipeline: The pipeline object that this stage is a part of.
            config: The hyperparameters to be used for this stage.
            status: A status object to report the progress of the computations.
            **inputs: The inputs of this stage. Each key-value pair represents an input field and the corresponding value.

        Returns:
            A dictionary containing the outputs of this stage.
            Each key-value pair in the dictionary represents an output field and the corresponding value.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """
        raise NotImplementedError()

    def configure(self, pipeline: Pipeline, input: Input, *args, **kwargs) -> dict:
        """
        Returns the rules to adopt hyperparameters based on the input data.

        Sometimes it can be necessary to automatically adopt hyperparameters based on the input data.
        For those cases where linear adoptation is suitable, this method can be overridden to return the rules which specify how to adopt the hyperparameters.
        The rules are then applied by the :meth:`repype.pipeline.Pipeline.configure` method.

        The rules must be specified by the following structure::

            {
                'key': [
                    factor,
                    default_user_factor,
                ],
            }

        The rules are resolved by mapping the above structure to the arguments of the :func:`repype.pipeline.create_config_entry` function.
        In this example, two new hyperparameters are created:
        
        #. The hyperparameter ``AF_key`` is created and defaults to the value of ``default_user_factor``.
        #. The hyperparameter ``key`` is created and defaults to the value of the hyperparameter ``AF_key`` times the value of ``factor``.

        In addition, a third element can be added to the list to further constrain the resulting values::

            {
                'key': [
                    factor,
                    default_user_factor,
                    {
                        type: 'float',
                        min: 0.0,
                        max: 1.0,
                    },
                ],
            }

        Arguments:
            pipeline: The pipeline object that this stage is a part of.
            input: The input to adopt the hyperparameters for.
            *args: Sequential arguments passed to :meth:`Pipeline.configure <repype.pipeline.Pipeline.configure>`.
            **kwargs: Keyword arguments passed to :meth:`Pipeline.configure <repype.pipeline.Pipeline.configure>`.
        """
        return dict()
    
    @property
    def signature(self) -> dict:
        """
        Get a serializable representation of the implementation of the stage.

        The signature contains the attributes and the methods of the stage.
        Methods are represented by their bytecode.
        Further callables beyond the direct methods of the object are not respected.
        If any of those changes, incrementing a ``signature_bump`` attribute should be considered.
        """
        signature = dict()

        # Iterate over all attributes of the stage (leaving out a few special ones)
        for key in dir(self):
            if key in ('__doc__', '__weakref__', '__module__', '__dict__', '__slotnames__', 'signature', 'sha'): continue
            value = getattr(self, key)

            if isinstance(value, Iterable):
                # Only keep the item if the iterable is is JSON-serializable
                try:
                    value = json.loads(json.dumps(list(value)))
                except TypeError:
                    continue

            if callable(value):
                # Only keep the item if has a custom implementation
                try:
                    value = value.__code__.co_code.hex()
                except AttributeError:
                    continue

            # Add the item to the signature
            signature[key] = value

        # Apply some "fixes" to the signature, apparently the order of the items is not guaranteed
        # - https://github.com/kostrykin/repype/pull/15#issuecomment-2293154385
        # - https://github.com/kostrykin/repype/pull/15#issuecomment-2293264509
        for key in ('inputs', 'outputs', 'consumes'):
            signature[key] = list(sorted(signature[key]))

        # Return the signature
        return signature

    @property
    def sha(self) -> str:
        """
        Get an SHA-1 hash which represents the implementation of this stage.

        The restrictions of the :attr:`signature` property apply.
        """
        signature_str = json.dumps(self.signature)
        return hashlib.sha1(signature_str.encode('utf-8')).hexdigest()

    def __str__(self) -> str:
        """
        Get a brief string representation of the stage (this is the stage ID).
        """
        return self.id

    def __repr__(self) -> str:
        return f'<{type(self).__name__}, id: {self.id}>'