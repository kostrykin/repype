from textual.binding import (
    Binding,
)
from textual.containers import (
    Vertical,
)
from textual.screen import (
    ModalScreen,
)
from textual.widgets import (
    Footer,
    Input,
    Header,
    Label,
    TextArea,
)
import yaml
from .confirm import ConfirmScreen


class EditorScreen(ModalScreen[bool]):

    BINDINGS = [
        Binding('ctrl+s', 'save', 'Save', priority = True),
        Binding('ctrl+c', 'cancel', 'Cancel', priority = True),
    ]

    def __init__(self, mode, task = None, parent_task = None):
        assert task or parent_task, (task, parent_task)
        assert not(task and parent_task), (task, parent_task)
        super().__init__()
        self.mode = mode
        self.my_task = task
        self.parent_task = parent_task

    @staticmethod
    def new(parent_task):
        screen = EditorScreen(mode = 'new', parent_task = parent_task)
        screen.sub_title = 'Add child task'
        return screen

    @staticmethod
    def edit(task):
        screen = EditorScreen(mode = 'edit', task = task)
        screen.sub_title = 'Edit task'
        return screen

    def compose(self):
        yaml_editor = TextArea.code_editor(language = 'yaml', id = 'editor-code')
        yaml_editor.indent_width = 2

        yield Header()

        with Vertical(id = 'editor-main'):

            if self.mode == 'new':
                yield Label(f'[bold]Parent task:[/bold] {str(self.parent_task.path)}', id = 'editor-main-header')
                yield Input(placeholder = 'Task name', restrict=r'[a-zA-Z0-9-_=¯., ]+', id = 'editor-main-name')
            else:
                yield Label(f'[bold]Task:[/bold] {str(self.my_task.path)}', id = 'editor-main-header')

            yield yaml_editor

        yield Footer()

    @property
    def task_name_input(self):
        assert self.mode == 'new'
        return self.query_one('#editor-main-name')

    @property
    def task_name(self):
        return self.task_name_input.value.strip()

    @property
    def task_spec_editor(self):
        return self.query_one('#editor-code')

    @property
    def task_spec(self):
        return self.task_spec_editor.text

    def on_mount(self):
        if self.my_task:
            with (self.my_task.path / 'task.yml').open('r') as task_file:
                self.task_spec_editor.text = task_file.read()

    def action_save(self):
        # Validate task name
        if self.mode == 'new':
            if len(self.task_name) == 0:
                self.app.notify('Invalid task name', severity='error', timeout=3)
                self.task_name_input.focus()
                return

        # Validate YAML code
        try:
            yaml.safe_load(self.task_spec)
        except yaml.error.YAMLError:
            self.app.notify('Invalid task specification', severity='error', timeout=3)
            self.task_spec_editor.focus()
            return

        # Validate that the YAML code is not empty
        if not self.task_spec.strip():
            self.app.notify('Task specification cannot be empty', severity='error', timeout=3)
            self.task_spec_editor.focus()
            return

        # Create a new task, if required
        if self.mode == 'new':
            task_path = self.parent_task.path / self.task_name
            assert not task_path.exists(), task_path
            task_path.mkdir(parents = True, exist_ok = True)
            with (task_path / 'task.yml').open('w') as task_file:
                task_file.write(self.task_spec)

        # Update the task, if required
        if self.mode == 'edit':
            with (self.my_task.path / 'task.yml').open('w') as task_file:
                task_file.write(self.task_spec)

        # Indicate success
        self.dismiss(True)

    def action_cancel(self):
        screen = ConfirmScreen('Close the task editor without saving?', default = 'no')
        def confirm(yes):
            if yes:
                self.dismiss(False)
        self.app.push_screen(screen, confirm)