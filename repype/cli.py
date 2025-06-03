import asyncio
import json
import os
import pathlib
import time

import repype.batch
import repype.status
import repype.task
from repype.typing import (
    Any,
    Coroutine,
    List,
    Optional,
    PathLike,
    Type,
    Union,
)


def format_hms(seconds):
    """
    Format a duration in seconds as hours, minutes, and seconds.
    """
    seconds = round(seconds)
    h, m, s = seconds // 3600, (seconds % 3600) // 60, (seconds % 60)
    ms = f'{m:02d}:{s:02d}'
    return ms if h == 0 else f'{h:d}:{ms}'


class StatusReaderConsoleAdapter(repype.status.StatusReader):
    """
    Writes formatted status updates to stdout.

    The status updates are indented according to the level of the nesting hierarchy. In addition, an empty line is
    printed when the level of indentation changes. Intermediate status updates can be muted by setting the environment
    variable ``REPYPE_CLI_INTERMEDIATE`` to ``0``.

    Sub-classes can be used to customize the formatting of status updates, or to add entirely new types of status
    updates:

    .. runblock:: pycon

        >>> import repype.cli
        >>>
        >>> class ExtendedStatusReader(repype.cli.StatusReaderConsoleAdapter):
        ...     def format(self, positions, status, intermediate):
        ...         if isinstance(status, dict) and status.get('info') == 'custom':
        ...             return f'*** {status["text"]} ({status["number"]})'
        ...         return super().format(positions, status, intermediate)
        >>>
        >>> async def main():
        ...     with repype.status.create() as status:
        ...         async with ExtendedStatusReader(status.filepath):
        ...             repype.status.update(status, info = 'custom', text = 'Text', number = 42)
        >>>
        >>> import asyncio
        >>> asyncio.run(main())

    Arguments:
        *args: Passed through to the base class.
        indent: Indentation level for each line of the status (for each level of the nesting hierarchy).
        batch: Batch instance used for running tasks.
        **kwargs: Passed through to the base class.
    """

    progress_bar_length = 20
    """
    Length of the progress bar displayed for :func:`repype.status.progress` status updates.
    """

    indent: int
    """
    Indentation level for each line of the status (for each level of the nesting hierarchy).
    """

    margin: Optional[str]
    """
    Last margin used for the status (corresponds to the total indentation).

    `None` if no status has been printed yet.
    """

    batch: Optional[repype.batch.Batch]
    """
    Batch instance used for running tasks.
    """

    def __init__(self, *args, indent: int = 2, batch: Optional[repype.batch.Batch] = None, **kwargs):
        self.indent = indent
        self._intermediate_line_length = 0
        self._progress_t0 = None
        self.margin = None
        self.batch = batch
        super().__init__(*args, **kwargs)

    def clear_line(self, line: str) -> str:
        """
        Clear any previously printed intermediate line by appending the proper number of spaces.
        """
        line = line.replace('\n', ' ')
        return line + ' ' * max((0, self._intermediate_line_length - len(line)))

    def handle_new_status(
            self,
            positions: List[int],
            status: Optional[Union[str, dict]],
            intermediate: bool,
        ) -> None:
        # If status is intermediate, ...
        if intermediate:

            # ...and intermediate statuses are not muted, ...
            if bool(json.loads(os.environ.get('REPYPE_CLI_INTERMEDIATE', '1'))):

                # ...clear the last line
                if status is None:
                    text = self.clear_line('')

                # ...print the last line of the status accordingly
                else:
                    text = self.full_format(positions, status, intermediate = True)

                lines = text.split('\n')
                if len(lines) > 1:
                    print('\n'.join(lines[:-1]))
                print(lines[-1], end='\r')
                self._intermediate_line_length = len(lines[-1])

        # Otherwise, print a regular line
        else:
            print(self.full_format(positions, status, intermediate = False))
            self._intermediate_line_length = 0

    def full_format(
            self,
            positions: List[int],
            status: Union[str, dict],
            intermediate: bool,
        ) -> str:
        """
        Format the status update as a string,
        including indentation and empty lines between blocks of different indentation.
        """
        try:
            text = str(self.format(positions, status, intermediate))
        except:  # noqa: E722
            print(f'An error occurred while processing status file: {self.filepath}')
            raise

        # Compute indentation, and add an extra line if the margin changes
        margin = ' ' * self.indent * (len(positions) - 1)
        if self.margin is not None and margin != self.margin and text.split('\n')[0].strip() != '':
            text = '\n' + text
        self.margin = margin

        # Indent all lines
        lines = [margin + line for line in text.split('\n')]
        lines[0] = self.clear_line(lines[0])
        return '\n'.join(lines)

    def format_dry_run_task(self, task_path: str) -> str:
        rc = self.batch.context(task_path)
        suffix = f' ({rc.pending})' if rc.pending else ''
        return f'- {task_path}{suffix}'

    def format(
            self,
            positions: List[int],
            status: Union[str, dict],
            intermediate: bool,
        ) -> str:
        """
        Format a status update as a string.
        """
        if isinstance(status, dict):

            text = None

            if status.get('info') == 'batch':
                text = '\n' f'{len(status["batch"])} task(s) selected for running'
                if not status['run']:
                    text += '\n' 'DRY RUN: use "--run" to run the tasks instead'
                    if status['batch']:
                        text += '\n\n' 'Selected tasks:\n' + '\n'.join(
                            self.format_dry_run_task(path) for path in status['batch']
                        )

            if status.get('info') == 'enter':
                text = f'\n({status["step"] + 1}/{status["step_count"]}) Entering task: {status["task"]}'

            if status.get('info') == 'start':
                if status['pickup'] or status['first_stage']:
                    text = f'Picking up from: {status["pickup"]} ({status["first_stage"] or "copy"})'
                else:
                    text = 'Starting from scratch'

            if status.get('info') == 'process':
                text = f'({status["step"] + 1}/{status["step_count"]}) Processing: {status["input_id"]}'

            if status.get('info') == 'start-stage':
                text = f'Running stage: {status["stage"]}'

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
                if status.get('step') == 0 or self._progress_t0 is None:
                    self._progress_t0 = time.time()
                    eta = ''
                else:
                    progress_t1 = time.time()
                    speed = (progress_t1 - self._progress_t0) / status.get('step')
                    eta = ', ETA: ' + format_hms(speed * (status.get('max_steps') - status.get('step')))
                text = f'{100 * status.get("step") / status.get("max_steps"):.1f}% '\
                    f'({status.get("step")} / {status.get("max_steps")}{eta})'
                progress_bar = ((self.progress_bar_length * status.get('step')) // status.get('max_steps')) * '='
                progress_bar = progress_bar + (self.progress_bar_length - len(progress_bar)) * ' '
                text = f'[{progress_bar}] {text}'
                if details := status.get('details'):
                    if isinstance(details, dict):
                        details = self.format_progress_details(details)
                    text = f'{details} {text}'
            else:
                self._progress_t0 = None

            return text if text else status

        else:
            self._progress_t0 = None
            return status

    def format_progress_details(self, details: dict) -> str:
        """
        Format the details of a progress status update as a string.
        """
        return str(details)


def run_cli_ex(*args, **kwargs) -> bool:
    """
    Run the command-line interface for batch processing, with options given explicitly.

    Arguments:
        *args: Passed through to :func:`main`.
        *kwargs: Passed through to :func:`main`.

    Returns:
        `True` if the batch processing was successful, `False` if an error occurred.
    """
    _main = main(*args, **kwargs)
    return asyncio.run(_main())


def run_cli(
        task_cls: Type[repype.task.Task] = repype.task.Task,
        status_reader_cls: Type[repype.status.StatusReader] = StatusReaderConsoleAdapter,
    ) -> bool:
    """
    Run the command-line interface for batch processing, parsing options from the command line.

    Arguments:
        task_cls: The task class to use for loading tasks.
        status_reader_cls: The status reader implementation to use for displaying status updates.

    Returns:
        `True` if the batch processing was successful, `False` if an error occurred.
    """

    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('path', help = 'Root directory for batch processing.')
    parser.add_argument('--task', help = 'Run only the given task.', type = str, default = list(), action = 'append')
    parser.add_argument('--task-dir', help = 'Run only the given task and those from its sub-directories.', type = str,
                        default = list(), action='append')
    op_mode = parser.add_mutually_exclusive_group()
    op_mode.add_argument('--run', help = 'Run the selected tasks.', action = 'store_true')
    op_mode.add_argument('--reset', help = 'Reset the selected tasks.', action = 'store_true')

    args = parser.parse_args()
    return repype.cli.run_cli_ex(
        args.path,
        args.run,
        args.reset,
        args.task,
        args.task_dir,
        task_cls,
        status_reader_cls,
    )


def main(
        path: PathLike,
        run: bool = False,
        reset: bool = False,
        tasks: List[PathLike] = list(),
        task_dirs: List[PathLike] = list(),
        task_cls: Type[repype.task.Task] = repype.task.Task,
        status_reader_cls: Type[repype.status.StatusReader] = StatusReaderConsoleAdapter,
    ) -> Coroutine[Any, Any, bool]:
    """
    Create a co-routine for running the command-line interface for batch processing.

    Arguments:
        path: The root directory for batch processing. Tasks will be loaded recursively from this directory.
        run: Whether to run the batch processing. If `False`, the tasks will be loaded, but not executed. Can only be
            `True` if `reset` is `False`.
        reset: Whether to reset the selected tasks. Can only be `True` if `run` is `False`.
        tasks: List of tasks to run. Tasks are identified by their paths. If given, only these tasks will be run.
        task_dirs: List of task directories to run. If given, only tasks from these directories and their
            sub-directories will be run.
        task_cls: The task class to use for loading tasks.
        status_reader_cls: The status reader implementation to use for displaying status updates.

    Raises:
        AssertionError: If `run` and `reset` are both `True`.

    Returns:
        Co-routine for batch processing. The co-routine returns `True` upon success, `False` if an error occurred.
    """
    assert not (run and reset), 'Cannot run and reset at the same time'

    path  = pathlib.Path(path).resolve()
    batch = repype.batch.Batch(task_cls)
    batch.load(path)

    if tasks or task_dirs:
        tasks     = [pathlib.Path(p).resolve() for p in tasks]
        task_dirs = [pathlib.Path(p).resolve() for p in task_dirs]
        contexts  = list()
        for rc in (batch.contexts if reset else batch.pending):
            if any(
                [
                    rc.task.path in tasks,
                    rc.task.path in task_dirs,
                ] + [
                    task_dir in rc.task.path.parents for task_dir in task_dirs
                ]
            ):
                contexts.append(rc)

    else:
        contexts = batch.pending

    # Define the main co-routine
    async def _main():

        # Reset the selected tasks
        if reset:
            print(f'\n{len(contexts)} task(s) selected:')
            print('\n'.join(f'- {rc.task.path}' for rc in contexts))

            confirm = input('\nReset the selected tasks? Enter the number of selected tasks to confirm: ')
            aborted = True
            try:
                if int(confirm) == len(contexts):
                    for rc in contexts:
                        rc.task.reset()
                    aborted = False
                    return True

            except ValueError:
                pass

            if aborted:
                print('Aborted.')
                return False

        # Otherwise, run the batch processing (dry or not dry)
        else:
            with repype.status.create() as status:

                repype.status.update(
                    status = status,
                    info = 'batch',
                    batch = [str(rc.task.path.resolve()) for rc in contexts],
                    run = run,
                )

                status_reader = status_reader_cls(status.filepath, batch = batch)
                async with status_reader:

                    if run:
                        return await batch.run(contexts, status = status)

                    else:
                        return True

    # Return the main co-routine
    return _main
