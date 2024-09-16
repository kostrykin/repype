import pathlib
import sys

if sys.version_info < (3, 11):
    from typing_extensions import *  # noqa: F403
else:
    from typing import *  # noqa: F403


Pipeline = TypeVar('..pipeline.Pipeline')
"""
Forward declaration of the :class:`repype.pipeline.Pipeline` class.
"""

PipelineData = Dict[str, Any]
"""
Pipeline data object. A dictionary with string keys and arbitrary values.
"""

InputID = TypeVar('InputID', int, str)
"""
Type hint for input identifiers. Each input identifier uniquely corresponds to a pipeline run.
"""

PathLike = TypeVar('PathLike', str, pathlib.Path)
"""
Type hint for path-like objects.
"""
