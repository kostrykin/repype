import asyncio
import glob
import multiprocessing
import multiprocessing.connection
import pathlib
import sys
import traceback

import dill
import yaml

import repype.config
import repype.pipeline
import repype.status
import repype.task
from repype.typing import (
    Dict,
    List,
    Optional,
    PathLike,
    Type,
)


class RunContext:
    """
    The pipeline and the hyperparameters used to run a task.

    Arguments:
        task: The task to run.
    """

    task: repype.task.Task
    """
    The task to run.
    """

    pipeline: repype.pipeline.Pipeline
    """
    The pipeline to run the task with.
    Defaults to :meth:`task.create_pipeline()<repype.task.Task.create_pipeline()>`.
    """

    config: repype.config.Config
    """
    The hyperparameters to run the task with.
    Defaults to :meth:`task.create_config()<repype.task.Task.create_config()>`.
    """

    pending: repype.task.PendingReason
    """
    If and why the task is pending, or not pending at all.

    See :meth:`repype.task.Task.is_pending` for possible values.
    """

    def __init__(self, task: repype.task.Task):
        assert task.runnable
        self.task = task
        self.pipeline = task.create_pipeline()
        self.config = task.create_config()
        self.pending = task.is_pending(self.pipeline, self.config)

    def run(self, *args, **kwargs) -> repype.task.TaskData:
        """
        Run the task.

        Arguments:
            args: Additional arguments to pass to the task.
            kwargs: Additional keyword arguments to pass to the task.

        Returns:
            The *task data object* returned by the task.
        """
        return self.task.run(self.config, pipeline = self.pipeline, *args, **kwargs)

    def __eq__(self, other: object) -> bool:
        return other is not None and all(
            (
                isinstance(other, type(self)),
                self.task == other.task,
                self.pipeline == other.pipeline,
                self.config == other.config,
                self.pending == other.pending,
            )
        )

    def __hash__(self) -> int:
        return hash((self.task, self.pipeline, self.config))

    def __repr__(self) -> str:
        return f'<{type(self).__name__} "{self.task.path})">'


def run_task_process(rc, status) -> int:
    """
    Run a task using specific :class:`RunContext` and :class:`repype.status.Status` objects inside a separate process.

    Arguments:
        exit_code: The connection to send the exit code to.
        args_serialized: The serialized arguments to run the task.
            This should be a tuple of the shape ``(rc, status)``, where ``rc`` is a :class:`RunContext` object
            and ``status`` is a :class:`repype.status.Status` object, serialized using dill.

    Returns:
        0 upon successful completion, and 1 indicates failure.
    """
    # Run the task and exit the child process
    try:
        rc.run(status = status)
        return 0  # Indicate success to the parent process

    # If an exception occurs, update the status and re-raise the exception
    except:  # noqa: E722
        error = sys.exc_info()[0]
        repype.status.update(
            status = status,
            info = 'error',
            task = str(rc.task.path.resolve()),
            traceback = traceback.format_exc(),
            stage = error.stage.id if isinstance(error, repype.pipeline.StageError) else None,
        )
        return 1  # Indicate a failure to the parent process


if __name__ == '__main__':
    rc, status = dill.load(sys.stdin.buffer)
    exit_code = run_task_process(rc, status)
    sys.stdout.write(str(exit_code))


class Batch:
    """
    A collection of tasks to run.
    Each task is uniquely identified by its path.

    Arguments:
        task_cls: The class to use for tasks. Defaults to :class:`repype.task.Task`.
    """

    tasks: Dict[pathlib.Path, repype.task.Task]
    """
    A dictionary of tasks, indexed by their path.
    """

    task_cls: Type[repype.task.Task]
    """
    The class to use for tasks.
    """

    task_process: Optional[multiprocessing.Process]
    """
    The process running the current task.
    """

    def __init__(self, task_cls: Type[repype.task.Task] = repype.task.Task):
        self.tasks = dict()
        self.task_cls = task_cls
        self.task_process = None

    @property
    def resolved_tasks(self) -> Dict[pathlib.Path, repype.task.Task]:
        """
        Get a dictionary of all tasks, indexed by their resolved path.
        """
        return {task.path.resolve(): task for task in self.tasks.values()}

    def task(self, path: PathLike, spec: Optional[dict] = None) -> Optional[repype.task.Task]:
        """
        Retrieve a task by its path.

        The task is loaded from the task specification if it has not been loaded before.
        Otherwise, the previously loaded task is returned.
        The task specification is either the `spec` argument, or the ``task.yml`` file in the task directory.
        The former is precedencial over the latter.

        The `path` argument is used to later:

        #. Identitfy the task using this method
        #. Establish parential relations, see :attr:`repype.task.Task.parent`
        #. Resolve filepaths, see :meth:`repype.pipeline.Pipeline.resolve`
        """
        assert path is not None
        path = pathlib.Path(path)
        task = self.resolved_tasks.get(path.resolve())

        # Using the spec argument overrides the spec file
        if spec is None:
            spec_filepath = path / 'task.yml'

            # If neither the spec argument was given, nor the spec file exists, return the previously loaded task
            if not spec_filepath.is_file():
                return task

            # If the spec file exists, load the spec
            with spec_filepath.open('r') as spec_file:
                spec = yaml.safe_load(spec_file)

        # Retrieve the parent task and instantiate the requested task
        if task is None:
            parent = self.task(path.parent) if path.parent else None
            task = self.task_cls(path = path, spec = spec, parent = parent)
            assert path not in self.tasks
            self.tasks[path] = task
            return task

        # Check whether the task has the right spec
        else:
            assert (
                task.spec == spec
            ), f'{path}: Requested specification {spec} does not match previously loaded specification {task.spec}'
            return task

    def load(self, root_path: PathLike) -> None:
        """
        Load all tasks from a directory tree.
        """
        root_path = pathlib.Path(root_path)
        assert root_path.is_dir()
        for path in glob.glob(str(root_path / '**/task.yml'), recursive = True):
            self.task(pathlib.Path(path).parent)

    @property
    def contexts(self) -> List[RunContext]:
        """
        Get a list of run contexts for all tasks.

        The list is sorted alphabetically by the task path.
        """
        contexts = [RunContext(task) for task in self.tasks.values() if task.runnable]
        return sorted(contexts, key = lambda rc: rc.task.path.resolve())

    @property
    def pending(self) -> List[RunContext]:
        """
        Get a list of run contexts for all pending tasks.
        """
        return [rc for rc in self.contexts if rc.pending]

    def context(self, path: PathLike) -> Optional[RunContext]:
        """
        Get a run context for a specific task.

        Returns:
            The run context for the task, or `None` if the task is not loaded.
        """
        for rc in self.contexts:
            if rc.task.path.resolve() == pathlib.Path(path).resolve():
                return rc
        return None

    async def run(
            self,
            contexts: Optional[List[RunContext]] = None,
            status: Optional[repype.status.Status] = None,
        ) -> bool:
        """
        Run all pending tasks (or a subset).

        Each task is run in a separate process using :meth:`run_task_process`.
        This ensures that each task runs with a clean environment, and no memory is leaked in between of tasks.

        Arguments:
            contexts: List of run contexts to run. Defaults to all pending tasks.
            status: The status object to update during task execution. Defaults to a new status object.

        Returns:
            `True` if all tasks were completed successfully, and `False` otherwise
        """
        assert self.task_process is None, 'A task is already running'
        try:

            contexts = self.pending if contexts is None else contexts
            contexts = sorted(contexts, key = lambda rc: rc.task.path.resolve())
            for rc_idx, rc in enumerate(contexts):
                task_status = repype.status.derive(status)

                repype.status.update(
                    status = task_status,
                    info = 'enter',
                    task = str(rc.task.path.resolve()),
                    step = rc_idx,
                    step_count = len(contexts),
                )

                # Run the task in a separate process
                self.task_process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    '-m'
                    'repype.batch',
                    stdin = asyncio.subprocess.PIPE,
                    stdout = asyncio.subprocess.PIPE,
                )
                stdout = (await self.task_process.communicate(input = dill.dumps((rc, task_status),)))[0]
                exit_code = int(stdout) if stdout else None
                if exit_code != 0:
                    repype.status.update(
                        status = status,
                        info = 'interrupted',
                        exit_code = exit_code,  # `exit_code` is None if the process was killed,
                    )                           # and 1 if an exception was raised in the child process

                    # Interrupt task execution due to an error
                    return False

            # All tasks were completed successfully
            return True

        finally:
            self.task_process = None

    async def cancel(self) -> None:
        """
        Cancel currently running tasks (if any).
        """
        if self.task_process:
            self.task_process.terminate()
            if self.task_process.returncode is not None:
                self.task_process.kill()
            await self.task_process.wait()
