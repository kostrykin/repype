import shutil

from textual.binding import (
    Binding,
)
from textual.screen import (
    Screen,
)
from textual.widgets import (
    Footer,
    Header,
    Static,
    Tree,
)
from .confirm import ConfirmScreen
from .editor import EditorScreen
from .run import RunScreen


def find_root_tasks(batch):
    for task in batch.tasks.values():
        if task.parent is None:
            yield task


def find_child_tasks(batch):
    tasks = sorted((task for task in batch.tasks.values() if task.parent), key = lambda task: len(str(task.path)))
    for task in tasks:
        yield task


def format_task_label(batch, task):
    pending_tasks = [rc.task.path for rc in batch.pending]
    label = str(task.path.relative_to(task.parent.path) if task.parent else task.path.resolve())
    if task.path in pending_tasks:
        return f'{label} [bold](pending)[/bold]'
    else:
        return f'{label}'


class BatchScreen(Screen):

    BINDINGS = [
        Binding('a', 'add_task', 'Add child task'),
        Binding('e', 'edit_task', 'Edit task'),
        Binding('d', 'delete_task', 'Delete task'),
        Binding('r', 'run_task', 'Run task'),
        Binding('R', 'reset_task', 'Reset task'),
    ]

    def __init__(self):
        super().__init__()
        self.sub_title = 'Manage tasks'
        self.task_tree = Tree('Loaded tasks', id = 'setup-tasks')

    def on_mount(self):
        self.task_tree.root.expand()
        self.update_task_tree()

    def update_task_tree(self):
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
        for task in find_child_tasks(self.app.batch):
            parent = task_nodes[task.parent]
            node = parent.add(format_task_label(self.app.batch, task), expand = True, data = task)
            task_nodes[task] = node

    def compose(self):
        yield Header()
        yield self.task_tree
        yield Static(id = 'batch-pending')
        yield Footer()

    def action_add_task(self):
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            screen = EditorScreen.new(parent_task = cursor.data)
            def add_task(ok):
                if ok:
                    self.update_task_tree()
            self.app.push_screen(screen, add_task)

    def action_edit_task(self):
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            screen = EditorScreen.edit(task = cursor.data)
            def update_task(ok):
                if ok:
                    self.update_task_tree()
            self.app.push_screen(screen, update_task)

    def action_delete_task(self):
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            screen = ConfirmScreen(
                'Delete the task and all sub-tasks?'
                '\n' '[bold]' + str(cursor.data.path) + '[/bold]',
                yes_variant = 'warning',
                default = 'no',
            )
            def confirm(yes):
                if yes:
                    shutil.rmtree(cursor.data.path)
                    self.update_task_tree()
            self.app.push_screen(screen, confirm)

    def action_run_task(self):
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

    def action_reset_task(self):
        cursor = self.task_tree.cursor_node
        if cursor and cursor.data:
            screen = ConfirmScreen(
                'Reset the task?'
                '\n' '[bold]' + str(cursor.data.path) + '[/bold]',
                yes_variant = 'warning',
                default = 'no',
            )
            def confirm(yes):
                if yes:
                    cursor.data.reset()
                    self.update_task_tree()
            self.app.push_screen(screen, confirm)