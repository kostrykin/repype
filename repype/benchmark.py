import pathlib

import pandas as pd

from repype.typing import (
    Generic,
    InputID,
    Iterable,
    PathLike,
    Self,
    Tuple,
    TypeVar,
)

ValueType = TypeVar('ValueType')


class Benchmark(Generic[ValueType]):

    df: pd.DataFrame
    """
    """

    filepath: pathlib.Path
    """
    """

    def __init__(self, filepath: PathLike):
        self.filepath = pathlib.Path(filepath)
        if self.filepath.is_file():
            self.df = pd.read_csv(self.filepath, index_col = 0)
        else:
            self.df = pd.DataFrame()

    def set(self, other: Self) -> Self:
        self.df = other.df.copy()
        return self

    def __getitem__(self, where: Tuple[str, InputID]) -> ValueType:
        stage_id, input_id = where
        return self.df.at[stage_id, input_id]

    def __setitem__(self, where: Tuple[str, InputID], value: ValueType) -> Self:
        stage_id, input_id = where
        self.df.at[stage_id, input_id] = value
        return self

    def retain(self, stage_ids: Iterable[str], input_ids: Iterable[InputID]) -> Self:
        """
        """

        # Keep only those `stage_ids` and `input_ids` that are present in the dataframe,
        stage_ids = frozenset(stage_ids) & frozenset(self.df.index)
        input_ids = frozenset(input_ids) & frozenset(self.df.columns)

        # Ensure that the order of the `stage_ids` and `input_ids` is preserved
        stage_ids = sorted(stage_ids, key = lambda stage_id: self.df.index.get_loc(stage_id))
        input_ids = sorted(input_ids, key = lambda input_id: self.df.columns.get_loc(input_id))

        # Select the subset of the dataframe corresponding to the `stage_ids` and `input_ids`
        self.df = self.df[list(input_ids)].transpose()[list(stage_ids)].transpose()
        return self

    def save(self) -> Self:
        self.df.to_csv(self.filepath)
        return self

    def __eq__(self, other: object) -> bool:
        return all(
            (
                isinstance(other, Benchmark),
                self.df.equals(other.df),
            )
        )
