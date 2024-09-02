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
"""
Represents the type of the benchmark values.
"""


class Benchmark(Generic[ValueType]):
    """
    Represents the benchmark data for a task.

    Arguments:
        filepath: The path to the file where the benchmark data is persisted.

    Example:

        .. runblock:: pycon

            >>> import tempfile
            >>> from repype.benchmark import Benchmark
            >>>
            >>> with tempfile.TemporaryDirectory() as tmp_path:
            ...     benchmark = Benchmark[float](tmp_path + '/benchmark.csv')
            ...     benchmark['stage1', 'input-1'] = 10
            ...     benchmark.save()
            ...     del benchmark
            ...     benchmark = Benchmark[float](tmp_path + '/benchmark.csv')
            ...     print(benchmark['stage1', 'input-1'])
    """

    df: pd.DataFrame
    """
    The benchmark data dataframe.
    """

    filepath: pathlib.Path
    """
    The path to the file where the benchmark data is persisted.
    """

    def __init__(self, filepath: PathLike):
        self.filepath = pathlib.Path(filepath)
        if self.filepath.is_file():
            self.df = pd.read_csv(self.filepath, index_col = 0)
        else:
            self.df = pd.DataFrame()

    def set(self, other: Self) -> Self:
        """
        Set the benchmark data to the benchmark data of another instance.
        """
        self.df = other.df.copy()
        return self

    def __getitem__(self, where: Tuple[str, InputID]) -> ValueType:
        """
        Get the benchmark value for a stage and an input.
        """
        stage_id, input_id = where
        return self.df.at[stage_id, input_id]

    def __setitem__(self, where: Tuple[str, InputID], value: ValueType) -> Self:
        """
        Set the benchmark `value` for a stage and an input.
        """
        stage_id, input_id = where
        self.df.at[stage_id, input_id] = value
        return self

    def retain(self, stage_ids: Iterable[str], input_ids: Iterable[InputID]) -> Self:
        """
        Retain only the benchmark data for the specified `stage_ids` and `input_ids`.
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

    def save(self) -> None:
        """
        Persist the benchmark data to :attr:`filepath`.
        """
        self.df.to_csv(self.filepath)

    def __eq__(self, other: object) -> bool:
        """
        Check if the benchmark data is equal to another instance.
        """
        return all(
            (
                isinstance(other, Benchmark),
                self.df.equals(other.df),
            )
        )
