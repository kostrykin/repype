import sys

import repype.batch
import repype.cli
import repype.status
from repype.typing import Type


def run_cli(
        task_cls: Type[repype.task.Task] = repype.task.Task,
        status_reader_cls: Type[repype.status.StatusReader] = repype.cli.StatusReaderConsoleAdapter,
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
    parser.add_argument('--run', help = 'Run batch processing.', action = 'store_true')
    parser.add_argument('--task', help = 'Run only the given task.', type = str, default = list(), action = 'append')
    parser.add_argument('--task-dir', help = 'Run only the given task and those from its sub-directories.', type = str,
                        default = list(), action='append')

    args = parser.parse_args()
    return repype.cli.run_cli(
        args.path,
        args.run,
        args.task,
        args.task_dir,
        task_cls,
        status_reader_cls,
    )


if __name__ == '__main__':
    if run_cli():
        sys.exit(0)
    else:
        sys.exit(1)
