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
    Type,
    Union,
)


class StatusReaderConsoleAdapter(pypers.status.StatusReader):
    """
    Writes status updates to stdout.
    """

    def __init__(self, *args, indent: int = 2, **kwargs):
        self.indent = indent
        self._intermediate_line_length = 0
        super().__init__(*args, **kwargs)

    def clear_line(self, line: str) -> str:
        line = line.replace('\n', ' ')
        return line + ' ' * max((0, self._intermediate_line_length - len(line)))

    def handle_new_status(self, parents: List[Union[str, dict]], positions: List[int], element: Optional[Union[str, dict]]):
        if element is not None:

            # Print an intermediate line
            if isinstance(element, dict) and element.get('content_type') == 'intermediate':
                line = self.format_line(parents, positions, element.get('content', ''), intermediate = True)
                print(line, end='\r')
                self._intermediate_line_length = len(line)

            # Print a regular line
            else:
                print(self.format_line(parents, positions, element, intermediate = False))
                self._intermediate_line_length = 0

        # Clear the intermediate line
        else:
            print(self.clear_line(''), end='\r')
            self._intermediate_line_length = 0

    def format_line(self, parents: List[Union[str, dict]], positions: List[int], status: Union[str, dict], intermediate: bool) -> str:
        text = str(self.format(parents, positions, status, intermediate))

        # Console output only supports single-line intermediates
        if intermediate:
            text = text.replace('\n', ' ')

        # Indent all lines
        margin = ' ' * self.indent * (len(positions) - 1)
        lines = [margin + line for line in text.split('\n')]
        lines[0] = self.clear_line(lines[0])
        return '\n'.join(lines)

    def format(self, parents: List[Union[str, dict]], positions: List[int], status: Union[str, dict], intermediate: bool) -> str:
        if isinstance(status, dict):

            text = None

            if status.get('info') == 'batch':
                 text = '\n' f'{len(status["batch"])} task(s) selected for running'
                 if not status['run']:
                     text += '\n' 'DRY RUN: use "--run" to run the tasks instead'

            if status.get('info') == 'enter':
                text = f'\n({status["step"] + 1}/{status["step_count"] + 1}) Entering task: {status["task"]}'

            if status.get('info') == 'start':
                if status['pickup'] or status['first_stage']:
                    text = f'Picking up from: {status["pickup"]} ({status["first_stage"]})'
                else:
                    text = 'Starting from scratch'

            if status.get('info') == 'storing':
                text = f'Storing results...'

            if status.get('info') == 'completed':
                text = f'Results have been stored'

            # FIXME: Handle `Status.progress` here

            return text if text else status
            
        else:
            return status


def run_cli(
        status_reader_cls: Type[pypers.status.StatusReader] = StatusReaderConsoleAdapter,
    ) -> bool:

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


def run_cli_ex(
        path: PathLike,
        run: bool = False,
        tasks: List[PathLike] = list(),
        task_dirs: List[PathLike] = list(),
        status_reader_cls: Type[pypers.status.StatusReader] = StatusReaderConsoleAdapter,
    ) -> bool:

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
            info = 'batch',
            batch = [str(rc.task.path.resolve()) for rc in contexts],
            run = run,
        )

        status_reader = status_reader_cls(status.filepath)
        with status_reader:

            if run:
                return batch.run(contexts, status = status)
            
            else:
                return True


if __name__ == '__main__':
    if run_cli():
        sys.exit(0)
    else:
        sys.exit(1)