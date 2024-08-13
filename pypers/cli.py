import argparse
import pathlib
import sys
import tempfile

import pypers.batch
import pypers.status
from pypers.typing import (
    List,
    PathLike,
)


class StdOutReader(pypers.status.StatusReader):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.cursor = []

    def on_modified(self, event) -> None:
        super().on_modified(event)
        self.update_output()

    # def get_data_element(self, cursor):
    #     parents = list()
    #     data = self.data
    #     for c in cursor:
    #         parents.append(data)
    #         data = data[c]
    #     return parents, data

    def get_parents(self, cursor):
        parents = list()
        data = self.data
        for c in cursor:
            parents.append(data)
            data = data[c]
        return parents

    def update_output(self):
        parents = self.get_parents(self.cursor)
        if len(parents) > 0 and self.cursor[-1] + 1 < parents[-1]:
            self.cursor[-1] += 1
            self.explore(parents[-1][self.cursor[-1]])


def run_cli() -> bool:

    if parser is None:
        import argparse
        parser = argparse.ArgumentParser()

    parser.add_argument('path', help='Root directory for batch processing.')
    parser.add_argument('--run', help='Run batch processing.', action='store_true')
    parser.add_argument('--task', help='Run only the given task.', type=str, default=[], action='append')
    parser.add_argument('--task-dir', help='Run only the given task and those from its sub-directories.', type=str, default=[], action='append')
    args = parser.parse_args()

    return run_cli_ex(
        args.path,
        args.run,
        args.task,
        args.task_dir,
    )


def run_cli_ex(path: PathLike, run: bool = False, tasks: List[PathLike] = list(), task_dirs: List[PathLike] = list()) -> bool:
    path  = pathlib.Path(path).resolve()
    batch = pypers.batch.Batch()
    batch.load(path)
    
    if tasks or task_dirs:
        tasks     = [pathlib.Path(p).resolve() for p in tasks]
        task_dirs = [pathlib.Path(p).resolve() for p in task_dirs]
        contexts  = list()
        for rc in batch.pending:
            if rc.task.path in tasks or any(task_dir in rc.task.path.parents for task_dir in task_dirs):
                contexts.append(rc)

    else:
        contexts = batch.pending

    with tempfile.TemporaryDirectory() as status_directory_path:
        status = pypers.status.Status(path = status_directory_path)
        pypers.status.update(
            status = status,
            batch = [str(rc.task.path.resolve()) for rc in contexts],
            run = run,
        )

        with StdOutReader(status.filepath):

            if run:
                return batch.run(contexts, status = status)
            
            else:
                return True


if __name__ == '__main__':
    if run_cli():
        sys.exit(0)
    else:
        sys.exit(1)