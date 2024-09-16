import yaml
from textual import work
from textual.app import App
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import (
    Footer,
    Header,
    Input,
    Label,
    TextArea,
)

import repype.task
from repype.typing import (
    Iterator,
    Literal,
    Optional,
    get_args,
)

from .confirm import confirm

ModeLiteral = Literal['new', 'edit']


class EditorScreen(ModalScreen[bool]):
    """
    A screen for editing or creating a task.

    Arguments:
        mode: The mode of the screen.
        task: The task to edit, if `mode` is `'edit'`.
        parent_task: The parent task of the new task, if `mode` is `'new'`.

    Raises:
        AssertionError: If the combination of arguments is invalid.
    """

    BINDINGS = [
        Binding('ctrl+s', 'save', 'Save', priority = True),
        Binding('ctrl+c', 'cancel', 'Cancel', priority = True),
    ]
    """
    Key bindings of the screen.
    """

    def __init__(
            self,
            mode: ModeLiteral,
            task: Optional[repype.task.Task] = None,
            parent_task: Optional[repype.task.Task] = None,
        ):
        assert mode in get_args(ModeLiteral), f'Invalid mode: "{mode}"'
        assert any(
            (
                mode == 'new' and task is None and parent_task is not None,
                mode == 'edit' and task is not None and parent_task is None,
            )
        ), (mode, task, parent_task)
        super().__init__()
        self.mode = mode
        self.my_task = task
        self.parent_task = parent_task

    @staticmethod
    async def new(app: App, parent_task: repype.task.Task) -> bool:
        """
        Display an editor screen to create a new sub-task.

        Arguments:
            app: The application instance.
            parent_task: The parent task of the sub-task.

        Returns:
            `True` if the task was created, and `False` otherwise.
        """
        screen = EditorScreen(mode = 'new', parent_task = parent_task)
        screen.sub_title = 'Add sub-task'
        return await app.push_screen_wait(screen)

    @staticmethod
    async def edit(app: App, task: repype.task.Task) -> bool:
        """
        Display an editor screen to update the `task` specification.

        Arguments:
            app: The application instance.
            task: The task to be updated.

        Returns:
            `True` if the task was saved, and `False` otherwise.
        """
        screen = EditorScreen(mode = 'edit', task = task)
        screen.sub_title = 'Edit task'
        return await app.push_screen_wait(screen)

    def compose(self) -> Iterator[Widget]:
        """
        Compose the screen.

        Yields:
            The components of the screen.
        """
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
    def task_name_input(self) -> Input:
        """
        The input widget for the task name.

        Raises:
            AssertionError: If the screen is not in `'new'` mode.
        """
        assert self.mode == 'new'
        return self.query_one('#editor-main-name')

    @property
    def task_name(self) -> str:
        """
        The task name.

        Raises:
            AssertionError: If the screen is not in `'new'` mode.
        """
        return self.task_name_input.value.strip()

    @property
    def task_spec_editor(self) -> TextArea:
        """
        The text area widget for the task specification.
        """
        return self.query_one('#editor-code')

    @property
    def task_spec(self) -> str:
        """
        The task specification.
        """
        return self.task_spec_editor.text

    def on_mount(self) -> None:
        """
        Loads the task specification, if the screen is in `'edit'` mode.
        """
        if self.my_task:
            with (self.my_task.path / 'task.yml').open('r') as task_file:
                self.task_spec_editor.text = task_file.read()

    def action_save(self) -> None:
        """
        Create a new task if the screen is in `'new'` mode, or update an existing task if the screen is in `'edit'`
        mode.

        Validates the input before saving the task. Shows an error if the task name is empty or the YAML code is
        invalid. Dismisses the screen with a value of `True` if the task is saved successfully.
        """
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

    @work
    async def action_cancel(self) -> None:
        """
        Close the task editor without saving.

        A confirmation dialog based on :meth:`confirm` is shown before closing the editor.

        Dismisses the screen with a value of `False` if the editor is closed.
        """
        if await self.confirm('Close the task editor without saving?', default = 'no'):
            self.dismiss(False)

    async def confirm(self, *args, **kwargs) -> bool:
        """
        Shortcut for :class:`repype.textual.confirm.confirm`.
        """
        return await confirm(self.app, *args, **kwargs)
