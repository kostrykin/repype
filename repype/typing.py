import pathlib
import sys

if sys.version_info < (3, 11):
    from typing_extensions import *
else:
    from typing import *


DataDictionary = Dict[str, Any]
FileID = TypeVar('FileID', int, str)
PathLike = TypeVar('PathLike', str, pathlib.Path)