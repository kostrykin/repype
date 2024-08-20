import pathlib
import sys

if sys.version_info < (3, 11):
    from typing_extensions import *
else:
    from typing import *


PipelineData = Dict[str, Any]
"""
Pipeline data object. A dictionary with string keys and arbitrary values.
"""

Input = TypeVar('Input', int, str)
"""
Type hint for input objects. Each input object corresponds uniquely to a pipeline run.
"""

PathLike = TypeVar('PathLike', str, pathlib.Path)
"""
Type hint for path-like objects.
"""