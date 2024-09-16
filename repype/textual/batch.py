import shutil

from textual import work
from textual.binding import Binding
from textual.screen import (
    ModalScreen,
    Screen,
)
from textual.widget import Widget
from textual.widgets import (
    Footer,
    Header,
    Static,
    Tree,
)

import repype.batch
import repype.task
from repype.typing import (
    Iterator,
    Type,
)

from .confirm import confirm
from .editor import EditorScreen
from .run import RunScreen


def find_root_tasks(batch: repype.batch.Batch) -> Iterator[repype.task.Task]:
    """
    Find all root tasks of the `batch`.

    Yields:
        The root tasks of the `batch`.
    """
    for task in batch.tasks.values():
        if task.parent is None:
            yield task


def find_sub_tasks(batch: repype.batch.Batch) -> Iterator[repype.task.Task]:
    """
    Find all non-root tasks of the `batch`.

    Yields:
        The non-root tasks of the `batch`.
    """
    tasks = sorted((task for task in batch.tasks.values() if task.parent), key = lambda task: len(str(task.path)))
    for task in tasks:
        yield task


class BatchScreen(Screen):
    """
    App screen for managing tasks.
    """

    BINDINGS = [
        Binding('a', 'add_task', 'Add sub-task'),
        Binding('e', 'edit_task', 'Edit task'),
        Binding('d', 'delete_task', 'Delete task'),
        Binding('r', 'run_tasks', 'Run tasks'),
        Binding('R', 'reset_task', 'Reset task'),
        Binding('x', 'toggle_task', 'Toggle task'),
    ]
    """
    Key bindings of the screen.
    """

    editor_screen_cls: Type[ModalScreen[bool]] = EditorScreen
    """
    The editor screen class.

    The return value of the editor screen is used to determine if the task tree should be updated. It should be `True`
    if changes were saved or tasks were created, and `False` otherwise.
    """

    run_screen_cls: Type[ModalScreen[int]] = RunScreen
    """
    The run screen class.

    The return value of the run screen should be the number of successfully completed tasks.
    """

    queued_tasks: list[repype.task.Task]
    """
    The tasks that are queued for running.
    """

    def __init__(self):
        super().__init__()
        self.sub_title = 'Manage tasks'
        self.task_tree = Tree('Loaded tasks', id = 'setup-tasks')
        self.queued_tasks = list()

    def on_mount(self) -> None:
        """
        Expands the root task nodes and loads the task tree.
        """
        self.task_tree.root.expand()
        self.update_task_tree()

    @property
    def non_pending_tasks(self) -> Iterator[repype.task.Task]:
        """
        Yields the non-pending tasks of the batch (i.e. completed and the non-runnable tasks).
        """
        for rc in self.app.batch.contexts:
            if rc not in self.app.batch.pending:
                yield rc.task

    def update_task_tree(self) -> None:
        """
        Reload the task tree.
        """
        self.app.batch.tasks.clear()
        self.app.batch.load(self.app.path)
        self.task_tree.clear()

        # Remove completed tasks from queued tasks
        non_pending_tasks = frozenset(self.non_pending_tasks)
        self.queued_tasks = [task for task in self.queued_tasks if task not in non_pending_tasks]
        self.update_summary()

        # Create root task nodes
        task_nodes = dict()
        for task in find_root_tasks(self.app.batch):
            node = self.task_tree.root.add(self.format_task_label(task), expand = True, data = task)
            task_nodes[task] = node

        # Create child task nodes
        for task in find_sub_tasks(self.app.batch):
            parent = task_nodes[task.parent]
            node = parent.add(self.format_task_label(task), expand = True, data = task)
            task_nodes[task] = node

    def update_summary(self) -> None:
        """
        Update the batch summary.
        """
        label_summary = self.query_one(f'#batch-summary')
        label_summary.update(
            f'[bold]Tasks:[/bold] '
            f'{len(self.queued_tasks)} queued'
            f' / '
            f'{len(self.app.batch.pending)} pending'
        )

    def compose(self) -> Iterator[Widget]:
        """
        Compose the screen.

        Yields:
            The components of the screen.
        """
        yield Header()
        yield self.task_tree
        yield Static(id = 'batch-summary')
        yield Footer()

    @work
    async def action_add_task(self) -> None:
        """
        Create a sub-task for the selected task using an editor of the :attr:`editor_screen_cls` type.

        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            if await self.editor_screen_cls.new(self.app, parent_task = cursor.data):
                self.update_task_tree()

    @work
    async def action_edit_task(self) -> None:
        """
        Edit the selected task using an editor of the :attr:`editor_screen_cls` type.

        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            if await self.editor_screen_cls.edit(self.app, task = cursor.data):
                self.update_task_tree()

    @work
    async def action_delete_task(self) -> None:
        """
        Delete the selected task and all sub-tasks.

        A confirmation dialog based on :meth:`confirm` is shown before resetting the task.

        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            if await self.confirm(
                    'Delete the task and all sub-tasks?'
                    '\n' '[bold]' + str(cursor.data.path) + '[/bold]',
                    yes_variant = 'warning',
                    default = 'no',
                ):
                shutil.rmtree(cursor.data.path)
                self.update_task_tree()

    @work
    async def action_run_tasks(self) -> None:
        """
        Run the queued tasks.

        An instance of the :attr:`run_screen_cls` is pushed to the screen stack for running the tasks.

        If no tasks are queued and the selected task is not pending, an error is shown. Does nothing if no tasks are
        queued and no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:

            # Gather the queued run contexts and the selected task context
            queued_contexts  = [rc for rc in self.app.batch.pending if rc.task in self.queued_tasks]
            selected_context = self.app.batch.context(cursor.data.path)

            # Determine the contexts to run
            if len(queued_contexts) == 0:
                if selected_context in self.app.batch.pending:
                    if await self.confirm(
                            'No tasks queued. Run the selected task?' '\n'
                            '[bold]' + str(selected_context.task.path) + '[/bold]',
                            default = 'yes',
                        ):
                        queued_contexts = [selected_context]

                else:
                    self.app.notify('No tasks queued', severity = 'error', timeout = 3)

            # Run the contexts
            if len(queued_contexts) > 0:
                screen = self.run_screen_cls(queued_contexts)
                if await self.app.push_screen_wait(screen) > 0:
                    self.update_task_tree()

    @work
    async def action_reset_task(self) -> None:
        """
        Reset the selected task.

        A confirmation dialog based on :meth:`confirm` is shown before resetting the task.

        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            if await self.confirm(
                    'Reset the task?'
                    '\n' '[bold]' + str(cursor.data.path) + '[/bold]',
                    yes_variant = 'warning',
                    default = 'no',
                ):
                cursor.data.reset()
                self.update_task_tree()

    async def action_toggle_task(self) -> None:
        """
        Toggle the selected task (queued or not queued).

        Does nothing if the task is not pending.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:

            # Toggle the task
            if cursor.data not in self.non_pending_tasks:
                if cursor.data in self.queued_tasks:
                    self.queued_tasks.remove(cursor.data)
                else:
                    self.queued_tasks.append(cursor.data)
                self.task_tree.cursor_node.label = self.format_task_label(cursor.data)
                self.update_summary()

            # Show an error if the task is not pending
            else:
                self.app.notify('Only pending tasks can be queued', severity = 'error', timeout = 3)

    async def confirm(self, *args, **kwargs) -> bool:
        """
        Shortcut for :class:`repype.textual.confirm.confirm`.
        """
        return await confirm(self.app, *args, **kwargs)

    def format_task_label(self, task: repype.task.Task) -> str:
        """
        Format the label of a `task` from the `batch` for display in the task tree.
        """
        pending_tasks = [rc.task.path for rc in self.app.batch.pending]
        label = str(task.path.relative_to(task.parent.path) if task.parent else task.path.resolve())
        queued_str = r'\[[bold]x[/bold]]' if task in self.queued_tasks else '[ ]'
        pending_str = '[bold](pending)[/bold]' if task.path in pending_tasks else ''
        return f'{queued_str} {label} {pending_str}'
