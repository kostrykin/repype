import glob
import os
import pathlib
import traceback

import pypers.pipeline
import pypers.config
import pypers.status
import pypers.task
from pypers.typing import (
    List,
    Optional,
    PathLike,
)
import yaml
    

class RunContext:
    """
    The pipeline and the hyperparameters used to run a task.
    """

    def __init__(self, task):
        assert task.runnable
        self.task = task
        self.pipeline = task.create_pipeline()
        self.config = task.create_config()


class Batch:

    def __init__(self):
        self.tasks = dict()

    def task(self, path: PathLike, spec: Optional[dict] = None) -> Optional[pypers.task.Task]:
        """
        Retrieve a task by its path.
        """
        path = pathlib.Path(path)
        task = self.tasks.get(path)

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
            task = pypers.task.Task(path = path, spec = spec, parent = parent)
            assert path not in self.tasks
            self.tasks[path] = task
            return task
        
        # Check whether the task has the right spec
        else:
            assert task.spec == spec, f'{path}: Requested specification {spec} does not match previously loaded specification {task.spec}'
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
        """
        return [RunContext(task) for task in self.tasks.values() if task.runnable]
    
    @property
    def pending(self) -> List[RunContext]:
        """
        Get a list of run contexts for all pending tasks.
        """
        return [rc for rc in self.contexts if rc.task.is_pending(rc.pipeline, rc.config)]

    def run(self, contexts: Optional[List[RunContext]] = None, status: Optional[pypers.status.Status] = None) -> bool:
        """
        Run all pending tasks.

        Each task is run in a forked process.
        This ensures that each task runs with a clean environment, and no memory is leaked in between of tasks.

        Returns:
            bool: True if all tasks were completed successfully, and False otherwise
        """
        contexts = self.pending if contexts is None else contexts
        for rc_idx, rc in enumerate(contexts):
            task_status = status.derive()
 
            pypers.status.update(
                status = task_status,
                info = 'enter',
                task = str(rc.task.path.resolve()),
                step = rc_idx,
                step_count = len(self.pending),
            )

            # Run the task in a forked process
            newpid = os.fork()
            if newpid == 0:

                # Run the task and exit the child process
                try:
                    rc.task.run(rc.config, pipeline = rc.pipeline, status = task_status)
                    os._exit(0)  # Indicate success to the parent process

                # If an exception occurs, update the status and re-raise the exception
                except:
                    pypers.status.update(
                        status = task_status,
                        info = 'error',
                        task = str(rc.task.path.resolve()),
                        traceback = traceback.format_exc(),
                    )
                    os._exit(1)  # Indicate a failure to the parent process

            # Wait for the child process to finish
            else:
                if os.waitpid(newpid, 0)[1] != 0:
                    pypers.status.update(
                        status = status,
                        info = 'interrupted',
                    )

                    # Interrupt task execution due to an error
                    return False

        # All tasks were completed successfully
        return True