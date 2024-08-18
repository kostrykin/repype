import pathlib
import sys

if sys.version_info < (3, 11):
    from typing_extensions import *
else:
    from typing import *


DataDictionary = Dict[str, Any]

Input = TypeVar('Input', int, str)
"""
Type hint for input objects. Each input object corresponds uniquely to a pipeline run.
"""

PathLike = TypeVar('PathLike', str, pathlib.Path)
"""
Type hint for path-like objects.
"""