from repype.typing import (
    Literal,
    Optional,
)

from textual import (
    on,
)
from repype.typing import (
    Iterator,
)
from textual.containers import (
    Container,
    Horizontal,
)
from textual.screen import (
    ModalScreen,
)
from textual.widget import (
    Widget,
)
from textual.widgets import (
    Button,
    Label,
)


DefaultLiteral = Literal['yes', 'no']


class ConfirmScreen(ModalScreen[bool]):
    """
    A screen that displays a confirmation dialog with a message and two buttons.

    Arguments:
        text: The text to display in the dialog.
        yes_text: The text to display on the "Yes" button.
        no_text: The text to display on the "No" button.
        yes_variant: The variant to use for the "Yes" button.
        no_variant: The variant to use for the "No" button.
        default: The button to focus by default.

    Raises:
        AssertionError: If the `default` argument is invalid.
    """

    text: str
    """
    The text to display in the dialog.
    """

    yes_text: str
    """
    The text to display on the "Yes" button.
    """

    no_text: str
    """
    The text to display on the "No" button.
    """

    yes_variant: str
    """
    The variant to use for the "Yes" button.
    """

    no_variant: str
    """
    The variant to use for the "No" button.
    """

    default: Optional[DefaultLiteral]
    """
    The button to focus by default.
    """

    def __init__(
            self,
            text: str,
            yes_text: str = 'Yes',
            no_text: str = 'No',
            yes_variant: str = 'primary',
            no_variant: str = 'default',
            default: Optional[DefaultLiteral] = None,
        ):
        assert default in (None, 'yes', 'no'), f'Invalid default value: "{default}"'
        super().__init__()
        self.text = text
        self.yes_text = yes_text
        self.no_text = no_text
        self.yes_variant = yes_variant
        self.no_variant = no_variant
        self.default = default

    def compose(self) -> Iterator[Widget]:
        """
        Compose the screen.

        Yields:
            The components of the screen.
        """
        with Container():
            yield Label(self.text)
            with Horizontal():
                yield Button(self.yes_text, id = 'yes', variant = self.yes_variant)
                yield Button(self.no_text, id = 'no', variant = self.no_variant)

    def on_mount(self) -> None:
        """
        Select the default button when the screen is mounted.
        """
        if self.default:
            self.query_one(f'#{self.default}').focus()

    @on(Button.Pressed, '#yes')
    def yes(self) -> None:
        """
        Handle the "Yes" button being pressed.

        Dismisses the dialog with a value of `True`.
        """
        self.dismiss(True)

    @on(Button.Pressed, '#no')
    def no(self) -> None:
        """
        Handle the "No" button being pressed.

        Dismisses the dialog with a value of `False`.
        """
        self.dismiss(False)