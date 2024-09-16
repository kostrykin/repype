import hashlib
import pathlib
import traceback

from textual import (
    log,
    work,
)
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import (
    Collapsible,
    Footer,
    Header,
    Label,
    ProgressBar,
)

import repype.batch
import repype.status
from repype.typing import (
    Iterable,
    Iterator,
    List,
    Optional,
    PathLike,
    Union,
)

from .confirm import confirm


class TaskUI:
    """
    Widgets of a task.

    Arguments:
        collapsible: The collapsible widget of the task.
    """

    collapsible: Collapsible
    """
    The collapsible widget of the task.
    """

    def __init__(self, collapsible: Collapsible):
        self.collapsible = collapsible

    @property
    def container(self) -> Vertical:
        """
        Container for permanent status updates.
        """
        return self.collapsible.query_one('.run-task-container')

    @property
    def intermediate(self) -> Vertical:
        """
        Container with widgets for intermediate status updates.

        Contains the :attr:`intermediate_label` and :attr:`intermediate_progressbar` widgets.
        """
        return self.collapsible.query_one('.run-task-intermediate')

    @property
    def intermediate_label(self) -> Label:
        """
        Label for intermediate status updates.
        """
        return self.intermediate.query_one('.run-task-intermediate > Label')

    @property
    def intermediate_progressbar(self) -> ProgressBar:
        """
        Progress bar for intermediate status updates.
        """
        return self.intermediate.query_one('.run-task-intermediate > ProgressBar')


class RunScreen(ModalScreen[int]):
    """
    Screen that performs a batch run of tasks, displaying the progress.

    Arguments:
        contexts: The contexts of the tasks to run.
    """

    BINDINGS = [
        Binding('ctrl+c', 'cancel', 'Cancel', priority = True),
        Binding('escape', 'close', 'Close'),
    ]
    """
    Key bindings of the screen.
    """

    contexts: list[repype.batch.RunContext]
    """
    The contexts of the tasks to run.
    """

    current_task_path: Optional[pathlib.Path]
    """
    The path of the current task being processed, or `None` if no task is being processed.
    """

    finished_tasks: set[str]
    """
    The set of finished tasks (represented by their resolved task paths).
    """

    success_count: int
    """
    The number of successfully completed tasks.
    """

    def __init__(self, contexts: Iterable[repype.batch.RunContext]):
        super().__init__()
        self.sub_title = 'Run tasks'
        self.contexts = list(contexts)
        self.current_task_path = None
        self.finished_tasks = set()
        self.success_count = 0

    def task_id(self, task_path: PathLike) -> str:
        """
        Get a unique alphanumeric identifier of a task.
        """
        assert task_path is not None
        task = self.app.batch.task(task_path)  # Resolve path identities
        return hashlib.sha1(str(task.path).encode('utf8')).hexdigest()

    def compose(self) -> Iterator[Widget]:
        """
        Compose the screen.

        Yields:
            The components of the screen.
        """
        yield Header()

        for rc in self.contexts:
            with Collapsible(collapsed = True, id = f'run-task-{self.task_id(rc.task.path)}'):

                container = Vertical(classes = 'run-task-container')
                container.styles.height = 'auto'
                yield container

                with Vertical(classes = 'run-task-intermediate') as intermediate:
                    intermediate.styles.height = 'auto'
                    intermediate.styles.display = 'none'
                    yield Label()
                    yield ProgressBar()

        yield Footer()

    def task_ui(self, task_path: PathLike) -> TaskUI:
        """
        Get the UI components of a task.
        """
        collapsible = self.query_one(f'#run-task-{self.task_id(task_path)}')
        return TaskUI(collapsible)

    def update_task_ui(self, task_path: PathLike) -> None:
        """
        Update the UI components of a task.
        """
        task = self.app.batch.task(task_path)
        task_ui = self.task_ui(task_path)
        task_ui.collapsible.title = str(task.path.resolve())
        if str(task_path) in self.finished_tasks:
            task_ui.collapsible.title += ' (done)'

    def on_mount(self) -> None:
        """
        Update the UI components of a task and run the batch.
        """
        for rc in self.contexts:
            self.update_task_ui(rc.task.path)
        self.run_batch()

    @work
    async def action_cancel(self) -> None:
        """
        Cancel the batch run.

        A confirmation dialog based on :meth:`confirm` is shown before cancellation.
        """
        if self.app.batch.task_process:
            if await self.confirm('Cancel the unfinished tasks?', default = 'no'):
                await self.app.batch.cancel()

    def action_close(self) -> None:
        """
        Close the screen, if no tasks are running.

        If tasks are running, an error message is shown.

        Dismisses the screen with the number of successfully completed tasks.
        """
        if self.app.batch.task_process is None:
            self.dismiss(self.success_count)
        else:
            self.app.notify('Cancel before closing, or wait to finish', severity='error', timeout=3)

    @work(exclusive = True)
    async def run_batch(self) -> None:
        """
        Run the batch of tasks.
        """
        with repype.status.create() as status:
            async with StatusReaderAdapter(status.filepath, self):
                success = await self.app.batch.run(self.contexts, status = status)
                log('RunScreen.run_batch', success = success)
                self.current_task_path = None

    def handle_new_status(
            self,
            positions: List[int],
            status: Optional[Union[str, dict]],
            intermediate: bool,
        ) -> None:
        """
        Process a new status update.

        The arguments are the same as those of the :meth:`~repype.status.StatusReader.handle_new_status` method of the
        :class:`repype.status.StatusReader` class.
        """
        log('RunScreen.handle_new_status', status = status, intermediate = intermediate)
        previous_task_path = self.current_task_path
        if isinstance(status, dict) and (task_path := status.get('task')):
            self.current_task_path = pathlib.Path(task_path)
        else:
            assert self.current_task_path is not None, f'status: {status}'
            task_path = self.current_task_path

        try:
            task_ui = self.task_ui(task_path)
            task_ui.collapsible.collapsed = False
            self.ends_with_rule = False

            # If the status is intermediate, update the corresponding widgets
            if intermediate:

                # If the status is None, hide the intermediate widgets
                if status is None:
                    task_ui.intermediate.styles.display = 'none'
                    return

                # Otherwise, show the intermediate widgets
                else:
                    task_ui.intermediate.styles.display = 'block'
                    target = task_ui.intermediate_label

                    # Update the intermediate progress bar
                    if isinstance(status, dict) and status.get('info') == 'progress':
                        task_ui.intermediate_progressbar.update(
                            progress = status.get('step'),
                            total = status.get('max_steps'),
                        )
                    else:
                        task_ui.intermediate_progressbar.update(progress = 0, total = None)

            # If the status is not intermediate, hide the intermediate widgets
            else:
                task_ui.intermediate.styles.display = 'none'
                target = Label()
                task_ui.container.mount(target)

            # Resolve dictionary-based status updates
            if isinstance(status, dict):

                if status.get('info') == 'enter':
                    if previous_task_path:
                        self.task_ui(previous_task_path).collapsible.collapsed = True
                    return

                if status.get('info') == 'start':
                    if status['pickup'] or status['first_stage']:
                        target.update(f'Picking up from: {status["pickup"]} ({status["first_stage"] or "copy"})')
                    else:
                        target.update('Starting from scratch')
                    return

                if status.get('info') == 'process':
                    target.update(
                        f'[bold]({status["step"] + 1}/{status["step_count"]})[/bold] '
                        f'Processing: {status["input_id"]}'
                    )
                    target.add_class('status-process')
                    return

                if status.get('info') == 'start-stage':
                    target.update(f'Running stage: {status["stage"]}')
                    return

                if status.get('info') == 'storing':
                    target.update(f'Storing results...')
                    return

                if status.get('info') == 'completed':
                    target.update(f'Results have been stored')
                    target.add_class('status-success')
                    self.finished_tasks.add(status['task'])
                    self.update_task_ui(status['task'])
                    self.success_count += 1
                    return

                if status.get('info') == 'error':
                    target.update('An error occurred:')
                    target.add_class('status-error')
                    task_ui.container.mount(Label(status['traceback']))
                    return

                if status.get('info') == 'progress':
                    target.update(str(status.get('details')))
                    return

                if status.get('info') == 'interrupted':
                    target.update('Batch run interrupted')
                    target.add_class('status-error')
                    return

            # Handle all remaining status updates
            target.update(self.custom_format(positions, status, intermediate))

        except:  # noqa: E722
            log('RunScreen.handle_new_status', error = traceback.format_exc())
            raise

    def custom_format(
            self,
            positions: List[int],
            status: Optional[Union[str, dict]],
            intermediate: bool,
        ) -> str:
        """
        Format a custom status update as a string.
        """
        return str(status)

    async def confirm(self, *args, **kwargs) -> bool:
        """
        Shortcut for :class:`repype.textual.confirm.confirm`.
        """
        return await confirm(self.app, *args, **kwargs)


class StatusReaderAdapter(repype.status.StatusReader):

    screen: RunScreen
    """
    The screen to which the status updates are delegated.

    Arguments:
        filepath: The path to the status file.
        screen: The screen to which the status updates are delegated.
    """

    def __init__(self, filepath: PathLike, screen: RunScreen):
        self.screen = screen
        super().__init__(filepath)

    def handle_new_status(self, *args, **kwargs):
        """
        Delegates the handling of new status updates to the
        :meth:`screen.handle_new_status <RunScreen.handle_new_status>` method.
        """
        self.screen.handle_new_status(*args, **kwargs)
