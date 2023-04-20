import pypers.pipeline

from collections.abc import Sequence


class DummyStage(pypers.pipeline.Stage):

    def __init__(self, name: str, inputs: Sequence, outputs: Sequence, consumes: Sequence, process: callable, configure: callable = None):
        super(DummyStage, self).__init__(
            name     = name,
            inputs   = inputs,
            outputs  = outputs,
            consumes = consumes,
        )
        self._process   = process
        self._configure = configure

    def process(self, input_data, *args, **kwargs):
        assert frozenset(input_data.keys()) == frozenset(self.inputs)
        if self._process is None: return dict()
        else: return self._process(input_data, *args, **kwargs)

    def configure(self, *args, **kwargs):
        if self._configure is None: return dict()
        else: return self._configure(*args, **kwargs)

