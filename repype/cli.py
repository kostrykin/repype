import argparse
import pathlib
import sys
import tempfile
import time

import repype.batch
import repype.status
from repype.typing import (
    List,
    Optional,
    PathLike,
    Type,
    Union,
)


def format_hms(seconds):
    seconds = round(seconds)
    h, m, s = seconds // 3600, (seconds % 3600) // 60, (seconds % 60)
    ms = f'{m:02d}:{s:02d}'
    return ms if h == 0 else f'{h:d}:{ms}'


class StatusReaderConsoleAdapter(repype.status.StatusReader):
    """
    Writes status updates to stdout.
    """

    progress_bar_length = 20

    def __init__(self, *args, indent: int = 2, **kwargs):
        self.indent = indent
        self._intermediate_line_length = 0
        self.margin = None
        super().__init__(*args, **kwargs)

    def clear_line(self, line: str) -> str:
        line = line.replace('\n', ' ')
        return line + ' ' * max((0, self._intermediate_line_length - len(line)))

    def handle_new_status(self, parents: List[Union[str, dict]], positions: List[int], element: Optional[Union[str, dict]]):
        # If status is intermediate, print the last line of the status accordingly
        if element.get('content_type') == 'intermediate':
            
            if element['content'] is None:
                text = ''
            else:
                text = self.full_format(parents, positions, element['content'][0], intermediate = True)

            lines = text.split('\n')
            if len(lines) > 1:
                print('\n'.join(lines[:-1]))
            print(lines[-1], end='\r')
            self._intermediate_line_length = len(lines[-1])

        # Print a regular line
        else:
            print(self.full_format(parents, positions, element, intermediate = False))
            self._intermediate_line_length = 0

    def full_format(self, parents: List[Union[str, dict]], positions: List[int], status: Union[str, dict], intermediate: bool) -> str:
        text = str(self.format(parents, positions, status, intermediate))

        # Compute indentation, and add an extra line if the margin changes
        margin = ' ' * self.indent * (len(positions) - 1)
        if self.margin is not None and margin != self.margin and text.split('\n')[0].strip() != '':
            text = '\n' + text
        self.margin = margin

        # Indent all lines
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
                text = f'\n({status["step"] + 1}/{status["step_count"]}) Entering task: {status["task"]}'

            if status.get('info') == 'start':
                if status['pickup'] or status['first_stage']:
                    text = f'Picking up from: {status["pickup"]} ({status["first_stage"] or "copy"})'
                else:
                    text = 'Starting from scratch'

            if status.get('info') == 'process':
                text = f'({status["step"] + 1}/{status["step_count"]}) Processing input: {status["input"]}'

            if status.get('info') == 'start-stage':
                text = f'Starting stage: {status["stage"]}'

            if status.get('info') == 'storing':
                text = f'Storing results...'

            if status.get('info') == 'completed':
                text = f'Results have been stored ✅'

            if status.get('info') == 'error':
                parts = ['\n🔴 An error occurred while processing']
                if status.get('stage') is not None:
                    parts.append(f'the stage "{status["stage"]} of')
                parts.append(f'the task {status["task"]}:\n')
                text = ' '.join(parts) + \
                    '-' * 80 + '\n' + \
                    status['traceback'] + \
                    '-' * 80
                
            if status.get('info') == 'interrupted':
                text = f'🔴 Batch run interrupted'

            if status.get('info') == 'progress':
                if status.get('step') == 0:
                    self.progress_t0 = time.time()
                    eta = ''
                else:
                    progress_t1 = time.time()
                    speed = (progress_t1 - self.progress_t0) / status.get('step')
                    eta = ', ETA: ' + format_hms(speed * (status.get('max_steps') - status.get('step')))
                text = f'{100 * status.get("step") / status.get("max_steps"):.1f}% ({status.get("step")} / {status.get("max_steps")}{eta})'
                progress_bar = ((self.progress_bar_length * status.get('step')) // status.get('max_steps')) * '='
                progress_bar = progress_bar + (self.progress_bar_length - len(progress_bar)) * ' '
                text = f'[{progress_bar}] {text}'
                if details := status.get('details'):
                    if isinstance(details, dict):
                        details = self.format_progress_details(details)
                    text = f'{details} {text}'

            return text if text else status
            
        else:
            return status
        
    def format_progress_details(self, details: dict) -> str:
        return str(details)


def run_cli(
        status_reader_cls: Type[repype.status.StatusReader] = StatusReaderConsoleAdapter,
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
        status_reader_cls,
    )


def run_cli_ex(
        path: PathLike,
        run: bool = False,
        tasks: List[PathLike] = list(),
        task_dirs: List[PathLike] = list(),
        task_cls: Type[repype.task.Task] = repype.task.Task,
        status_reader_cls: Type[repype.status.StatusReader] = StatusReaderConsoleAdapter,
    ) -> bool:

    path  = pathlib.Path(path).resolve()
    batch = repype.batch.Batch(task_cls)
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
        status = repype.status.Status(path = status_directory_path)
        repype.status.update(
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