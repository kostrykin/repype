import pypers.pipeline

from collections.abc import Sequence


# __init__(self, name: str, inputs: Sequence, outputs: Sequence, consumes: Sequence, process: callable, configure: callable = None):

def create_stage(**kwargs):
    kwargs = dict(kwargs)

    kwargs.setdefault('inputs'  , [])
    kwargs.setdefault('outputs' , [])
    kwargs.setdefault('consumes', [])

    _process   = kwargs.get('process'  , None)
    _configure = kwargs.get('configure', None)

    class DummyStage(pypers.pipeline.Stage):

        id       = kwargs['id']
        inputs   = kwargs['inputs']
        outputs  = kwargs['outputs']
        consumes = kwargs['consumes']

        def process(self, *args, **kwargs):
            assert frozenset(self.inputs).issubset(frozenset(kwargs.keys()))
            if _process is None: return dict()
            else: return _process(*args, **kwargs)

        def configure(self, *args, **kwargs):
            if _configure is None: return dict()
            else: return _configure(*args, **kwargs)

    return DummyStage()


# Test create_stage:

_stage = create_stage(id = 'dummy', inputs = ['x1', 'x2'], outputs = ['y'])
assert isinstance(_stage, pypers.pipeline.Stage)
assert isinstance(_stage.inputs , frozenset)
assert isinstance(_stage.outputs, frozenset)
assert _stage.inputs  == frozenset(['x1', 'x2'])
assert _stage.outputs == frozenset(['y'])
del _stage
