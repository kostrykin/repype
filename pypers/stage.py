import hashlib
import json
import pathlib
import re
import time

import pypers.config
import pypers.status
from pypers.typing import (
    DataDictionary,
    Dict,
    Optional,
    Protocol,
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
    def __call__(self, stage: 'Stage', event: StageEvent, *args, **kwargs) -> None: ...


def suggest_stage_id(class_name: str) -> str:
    """
    Suggest stage ID based on a class name.

    This function validates the class name, then finds and groups tokens in the class name.
    Tokens are grouped if they are consecutive and alphanumeric, but do not start with numbers.
    The function then converts the tokens to lowercase, removes underscores, and joins them with hyphens.

    :param class_name: The name of the class to suggest a configuration namespace for.
    :type class_name: str
    :return: A string of hyphen-separated tokens from the class name.
    :rtype: str
    :raises AssertionError: If the class name is not valid.
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

    Each stage can be controlled by a separate set of hyperparameters. Refer to the documentation of the respective pipeline stages for details. Most hyperparameters reside in namespaces, which are uniquely associated with the corresponding pipeline stages.

    :param id: The stage ID, used as the hyperparameter namespace. Defaults to the result of the :py:meth:`~.suggest_stage_id` function if not specified.
    :param inputs: List of inputs required by this stage.
    :param consumes: List of inputs consumed by this stage (cannot be used by subsequent stages).
    :param outputs: List of outputs produced by this stage.

    Automation
    ^^^^^^^^^^

    Hyperparameters can be set automatically using the :py:meth:`~.configure` method.

    Inputs and outputs
    ^^^^^^^^^^^^^^^^^^

    Each stage must declare its required inputs and the outputs it produces. These are used by :py:meth:`~.create_pipeline` to automatically determine the stage order. The input ``input`` is provided by the pipeline itself.
    """

    inputs   = []
    outputs  = []
    consumes = []
    enabled_by_default = True

    def __init__(self):
        self.id       = type(self).id if hasattr(type(self), 'id') else suggest_stage_id(type(self).__name__)
        self.inputs   = frozenset(type(self).inputs) | frozenset(type(self).consumes)
        self.outputs  = frozenset(type(self).outputs)
        self.consumes = frozenset(type(self).consumes)
        self.enabled_by_default = type(self).enabled_by_default
        assert not self.id.endswith('+'), 'The suffix "+" is reserved as an indication of "the stage after that stage"'
        self.event_callbacks: Dict[StageEvent, List[StageCallback]] = dict()

    def trigger_event(self, event: StageEvent, *args, **kwargs) -> None:
        if event in self.event_callbacks:
            for callback in self.event_callbacks[event]:
                callback(self, event, *args, **kwargs)

    def add_callback(self, event: StageEvent, callback: StageCallback) -> None:
        if event == 'after':
            self.add_callback( 'end', callback)
            self.add_callback('skip', callback)
        else:
            if event not in self.event_callbacks:
                self.event_callbacks[event] = list()
            self.event_callbacks[event].append(callback)

    def remove_callback(self, event: StageEvent, callback: StageCallback) -> None:
        if event == 'after':
            self.remove_callback( 'end', callback)
            self.remove_callback('skip', callback)
        else:
            if event in self.event_callbacks:
                self.event_callbacks[event].remove(callback)

    def __call__(
            self,
            data: DataDictionary,
            config: pypers.config.Config,
            status: Optional[pypers.status.Status] = None,
            log_root_dir: Optional[pathlib.Path] = None,
            **kwargs) -> float:
        config = config.get(self.id, {})

        # Run the stage if it is enabled
        if config.get('enabled', self.enabled_by_default):
            pypers.status.update(
                status = status,
                info = 'start-stage',
                stage = self.id,
                intermediate = True,
            )
            self.trigger_event('start', data, status = status, **kwargs)

            # Extract the input data of the stage
            input_data = {key: data[key] for key in self.inputs}

            # Clean up the hyperparameters passed to the stage implementation
            clean_config = config.copy()
            clean_config.pop('enabled', None)

            # Run the stage and measure the run time
            t0 = time.time()
            output_data = self.process(
                config = clean_config,
                log_root_dir = log_root_dir,
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
            self.trigger_event('end', data, status = status, **kwargs)
            return dt
        
        # Skip the stage
        else:
            self.skip(data, status = status, **kwargs)
            return 0.
        
    def skip(self, data, status = None, **kwargs):
        pypers.status.update(
            status = status,
            info = 'skip-stage',
            stage = self.id,
            intermediate = True,
        )
        self.trigger_event('skip', data, status = status, **kwargs)

    def process(
            self,
            config: Optional[pypers.config.Config] = None,
            log_root_dir: Optional[pathlib.Path] = None,
            status: Optional[pypers.status.Status] = None,
            **inputs,
        ) -> DataDictionary:
        """
        Executes the current pipeline stage.

        This method runs the current stage of the pipeline with the provided inputs, configuration parameters, and logging settings. It then returns the outputs produced by this stage.

        :param input_data: A dictionary containing the inputs required by this stage. Each key-value pair in the dictionary represents an input name and its corresponding value.
        :param config: A :py:class:`~pypers.config.Config` object, containing the hyperparameters to be used by this stage.
        :param log_root_dir: The path to the directory where log files will be written. If this parameter is ``None``, no log files will be written.
        :param status: A :py:class:`~pypers.status.Status` object.
        :return: A dictionary containing the outputs produced by this stage. Each key-value pair in the dictionary represents an output name and its corresponding value.
        """
        raise NotImplementedError()

    def configure(self, *args, **kwargs) -> dict:
        # FIXME: add documentation
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
            if key in ('__doc__', '__weakref__', '__module__', '__dict__', 'signature', 'sha'): continue
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

        # Return the signature
        return signature

    @property
    def sha(self) -> str:
        """
        Get a SHA-1 hash which represents the implementation of this stage.

        The restrictions of the :py:attr:`~.signature` property apply.
        """
        signature_str = json.dumps(self.signature)
        return hashlib.sha1(signature_str.encode('utf-8')).hexdigest()

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return f'<{type(self).__name__}, id: {self.id}>'