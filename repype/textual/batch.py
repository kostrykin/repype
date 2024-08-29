import shutil

import repype.task
import repype.batch
from repype.typing import (
    Iterator,
)
from textual import (
    work,
)
from textual.binding import (
    Binding,
)
from textual.screen import (
    Screen,
)
from textual.widget import (
    Widget,
)
from textual.widgets import (
    Footer,
    Header,
    Static,
    Tree,
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


def format_task_label(batch: repype.batch.Batch, task: repype.task.Task) -> str:
    """
    Format the label of a `task` from the `batch` for display in the task tree.
    """
    pending_tasks = [rc.task.path for rc in batch.pending]
    label = str(task.path.relative_to(task.parent.path) if task.parent else task.path.resolve())
    if task.path in pending_tasks:
        return f'{label} [bold](pending)[/bold]'
    else:
        return f'{label}'


class BatchScreen(Screen):
    """
    App screen for managing tasks.
    """

    BINDINGS = [
        Binding('a', 'add_task', 'Add child task'),
        Binding('e', 'edit_task', 'Edit task'),
        Binding('d', 'delete_task', 'Delete task'),
        Binding('r', 'run_task', 'Run task'),
        Binding('R', 'reset_task', 'Reset task'),
    ]
    """
    Key bindings for the screen.
    """

    def __init__(self):
        super().__init__()
        self.sub_title = 'Manage tasks'
        self.task_tree = Tree('Loaded tasks', id = 'setup-tasks')

    def on_mount(self) -> None:
        """
        Expands the root task nodes and loads the task tree.
        """
        self.task_tree.root.expand()
        self.update_task_tree()

    def update_task_tree(self) -> None:
        """
        Reload the task tree.
        """
        self.app.batch.tasks.clear()
        self.app.batch.load(self.app.path)
        self.task_tree.clear()

        # Update number of pending tasks
        label_pending = self.query_one(f'#batch-pending')
        label_pending.update(f'Tasks: {str(len(self.app.batch.pending))} pending')

        # Create root task nodes
        task_nodes = dict()
        for task in find_root_tasks(self.app.batch):
            node = self.task_tree.root.add(format_task_label(self.app.batch, task), expand = True, data = task)
            task_nodes[task] = node

        # Create child task nodes
        for task in find_sub_tasks(self.app.batch):
            parent = task_nodes[task.parent]
            node = parent.add(format_task_label(self.app.batch, task), expand = True, data = task)
            task_nodes[task] = node

    def compose(self) -> Iterator[Widget]:
        """
        Compose the screen.

        Yields:
            The components of the screen.
        """
        yield Header()
        yield self.task_tree
        yield Static(id = 'batch-pending')
        yield Footer()

    def action_add_task(self) -> None:
        """
        Add a sub-task for the selected task.

        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            screen = EditorScreen.new(parent_task = cursor.data)
            def add_task(ok):
                if ok:
                    self.update_task_tree()
            self.app.push_screen(screen, add_task)

    def action_edit_task(self) -> None:
        """
        Edit the selected task.
        
        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            screen = EditorScreen.edit(task = cursor.data)
            def update_task(ok):
                if ok:
                    self.update_task_tree()
            self.app.push_screen(screen, update_task)

    @work
    async def action_delete_task(self) -> None:
        """
        Delete the selected task and all sub-tasks.

        A confirmation dialog based on :class:`.ConfirmScreen` is shown before deleting the task.

        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            if await confirm(
                    self.app,
                    'Delete the task and all sub-tasks?'
                    '\n' '[bold]' + str(cursor.data.path) + '[/bold]',
                    yes_variant = 'warning',
                    default = 'no',
                ):
                shutil.rmtree(cursor.data.path)
                self.update_task_tree()

    def action_run_task(self) -> None:
        """
        Run the selected task.

        An instance of the :class:`.RunScreen` is pushed to the screen stack for running the task.

        If the task is not pending, a notification is shown.
        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            contexts = [rc for rc in self.app.batch.pending if rc.task.path == cursor.data.path]
            if len(contexts) == 0:
                self.app.notify('No pending tasks selected', severity = 'error', timeout = 3)
            else:
                screen = RunScreen(contexts)
                def update_task_tree(ok):
                    self.update_task_tree()
                self.app.push_screen(screen, update_task_tree)

    @work
    async def action_reset_task(self) -> None:
        """
        Reset the selected task.

        A confirmation dialog based on :class:`.ConfirmScreen` is shown before resetting the task.

        Does nothing if no task is selected.
        """
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            if await confirm(
                    self.app,
                    'Reset the task?'
                    '\n' '[bold]' + str(cursor.data.path) + '[/bold]',
                    yes_variant = 'warning',
                    default = 'no',
                ):
                cursor.data.reset()
                self.update_task_tree()