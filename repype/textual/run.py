import hashlib
import traceback
import types

import repype.status
from repype.typing import (
    Iterator,
    Optional,
    PathLike,
)
from textual import (
    log,
    work,
)
from textual.binding import (
    Binding,
)
from textual.containers import (
    Vertical,
)
from textual.css.query import (
    NoMatches,
)
from textual.screen import (
    ModalScreen,
)
from textual.widget import (
    Widget,
)
from textual.widgets import (
    Collapsible,
    Footer,
    Header,
    Label,
    ProgressBar,
)
from .confirm import confirm


class StatusReaderAdapter(repype.status.StatusReader):

    def __init__(self, filepath, run_screen):
        self.screen = run_screen
        super().__init__(filepath)

    def handle_new_status(self, *args, **kwargs):
        self.screen.handle_new_status(*args, **kwargs)


class RunScreen(ModalScreen[bool]):

    BINDINGS = [
        Binding('ctrl+c', 'cancel', 'Cancel', priority = True),
        Binding('escape', 'close', 'Close'),
    ]
    """
    Key bindings of the screen.
    """

    finished_tasks: set[str]
    """
    The set of finished tasks (represented by their resolved task paths).
    """

    def __init__(self, contexts):
        super().__init__()
        self.sub_title = 'Run tasks'
        self.contexts = contexts
        self.current_task_path = None
        self.intermediate = None
        self.intermediate_extra = ProgressBar()
        self.success = False
        self.finished_tasks = set()

    def task_id(self, task_path: PathLike) -> str:
        """
        Get a unique alphanumeric identifier of a task.
        """
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
                vertical = Vertical(classes = 'run-task-container')
                vertical.styles.height = 'auto'
                yield vertical

        yield Footer()

    def task_ui(self, task_path: PathLike) -> types.SimpleNamespace:
        """
        Get the UI components of a task.

        Returns:
            A namespace with the `collapsible` and `container` widgets.
        """
        collapsible = self.query_one(f'#run-task-{self.task_id(task_path)}')
        container = collapsible.query_one('.run-task-container')
        return types.SimpleNamespace(
            collapsible = collapsible,
            container = container,
        )

    def task_ui_or_none(self, task_path: PathLike) -> Optional[types.SimpleNamespace]:
        """
        Get the UI components of a task, or `None` if they do not exist.
        """
        try:
            return self.task_ui(task_path)
        except NoMatches:
            return None

    def update_task_state(self, task_path: PathLike) -> None:
        task = self.app.batch.task(task_path)
        task_ui = self.task_ui(task_path)
        task_ui.collapsible.title = str(task.path.resolve())
        if str(task_path) in self.finished_tasks:
            task_ui.collapsible.title += ' (done)'

    def on_mount(self):
        for rc in self.contexts:
            self.update_task_state(rc.task.path)
        self.run_batch()

    @work
    async def action_cancel(self):
        if self.app.batch.task_process:
            if await confirm(self.app, 'Cancel the unfinished tasks?', default = 'no'):
                await self.app.batch.cancel()

    def action_close(self):
        if self.app.batch.task_process is None:
            self.dismiss(self.success)
        else:
            self.app.notify('Cancel before closing, or wait to finish', severity='error', timeout=3)

    @work(exclusive = True)
    async def run_batch(self):
        with repype.status.create() as status:
            async with StatusReaderAdapter(status.filepath, self):
                success = await self.app.batch.run(self.contexts, status = status)

                # Report the success of the batch run
                log('StatusReader.run_batch', success = success)
                self.success = success

    def handle_new_status(self, parents, positions, status, intermediate):
        log('StatusReader.handle_new_status', status = status, intermediate = intermediate)
        if isinstance(status, dict) and (task_path := status.get('task')):
            self.current_task_path = task_path
        else:
            task_path = self.current_task_path

        task_ui = self.task_ui_or_none(task_path)
        try:
            if task_ui:
                task_ui.collapsible.collapsed = False
                self.ends_with_rule = False

            # If the new status is intermediate...
            if intermediate:

                # ...and empty, then clear the previous intermediate status
                if status is None and self.intermediate:
                    self.intermediate.remove()
                    self.intermediate = None
                    self.intermediate_extra.remove()
                    return

                # ...the previous was *not* intermediate, creata a new label
                elif self.intermediate is None:
                    label = Label()
                    self.intermediate = label
                    self.intermediate_extra.update(progress = 0, total = None)
                    task_ui.container.mount(self.intermediate)
                    task_ui.container.mount(self.intermediate_extra)

                # ...the previous was intermediate too, reuse its label
                else:
                    label = self.intermediate

            # If the new status is *not* intermediate, but the previous status *was* intermediate, reuse its label
            elif self.intermediate:
                label = self.intermediate
                self.intermediate = None
                self.intermediate_extra.remove()

            # If the new status is *not* intermediate, and the previous wasn't either, create a new label
            else:
                label = Label()
                task_ui.container.mount(label)

            # Resolve dictionary-based status updates
            if isinstance(status, dict):

                if status.get('info') == 'enter':
                    #label.update('Task has begun')
                    #self.update_intermediate_extra(task_ui.container, status = status, intermediate = intermediate)
                    return

                if status.get('info') == 'start':
                    if status['pickup'] or status['first_stage']:
                        label.update(f'Picking up from: {status["pickup"]} ({status["first_stage"] or "copy"})')
                    else:
                        label.update('Starting from scratch')
                    return

                if status.get('info') == 'process':
                    label.update(f'[bold]({status["step"] + 1}/{status["step_count"]})[/bold] Processing: {status["input_id"]}')
                    label.add_class('status-process')
                    return

                if status.get('info') == 'start-stage':
                    label.update(f'Starting stage: {status["stage"]}')
                    return

                if status.get('info') == 'storing':
                    label.update(f'Storing results...')
                    return

                if status.get('info') == 'completed':
                    label.update(f'Results have been stored')
                    label.add_class('status-success')
                    self.finished_tasks.add(status['task'])
                    self.update_task_state(status['task'])
                    return

                if status.get('info') == 'error':
                    label.update('An error occurred:')
                    label.add_class('status-error')
                    task_ui.container.mount(Label(status['traceback']))
                    return

                if status.get('info') == 'progress':
                    label.update(str(status.get('details')))
                    self.intermediate_extra.update(progress = status['step'], total = status['max_steps'])
                    return
                
                if status.get('info') == 'interrupted':
                    label.update('Batch run interrupted')
                    label.add_class('status-error')
                    return

            # Handle all remaining status updates
            label.update(str(status))

        except:
            log('StatusReader.handle_new_status', error = traceback.format_exc())
            raise