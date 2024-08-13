import argparse
import pathlib
import sys
import tempfile

import pypers.batch
import pypers.status
from pypers.typing import (
    List,
    Optional,
    PathLike,
    Union,
)


class StatusReaderConsoleAdapter(pypers.status.StatusReader):

    def __init__(self, *args, indent: int = 2, **kwargs):
        self.indent = indent
        self._intermediate_line_length = 0
        super().__init__(*args, **kwargs)

    def _clear_line(self, line: str) -> str:
        line = line.replace('\n', ' ')
        return line + ' ' * max((0, self._intermediate_line_length - len(line)))

    def handle_new_data(self, parents: List[Union[str, dict]], positions: List[int], element: Optional[Union[str, dict]]):
        margin = ' ' * self.indent * len(positions)
        if element is not None:

            # Print an intermediate line
            if isinstance(element, dict) and element.get('content_type') == 'intermediate':
                line = margin + str(element['content'])  # FIXME: Handle `Status.progress`
                print(self._clear_line(line), end='\r')
                self._intermediate_line_length = len(line)

            # Print a regular line
            else:
                print(self._clear_line(margin + str(element)), end='\r')
                self._intermediate_line_length = 0

        # Clear the intermediate line
        else:
            print(self._clear_line(''), end='\r')
            self._intermediate_line_length = 0


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

        with StatusReaderConsoleAdapter(status.filepath):

            if run:
                return batch.run(contexts, status = status)
            
            else:
                return True


if __name__ == '__main__':
    if run_cli():
        sys.exit(0)
    else:
        sys.exit(1)