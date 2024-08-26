from textual import (
    on,
)
from textual.containers import (
    Container,
    Horizontal,
)
from textual.screen import (
    ModalScreen,
)
from textual.widgets import (
    Button,
    Label,
)


class ConfirmScreen(ModalScreen[bool]):

    def __init__(self, text, yes_text = 'Yes', no_text = 'No', yes_variant = 'primary', no_variant = 'default', default = None):
        super().__init__()
        self.text = text
        self.yes_text = yes_text
        self.no_text = no_text
        self.yes_variant = yes_variant
        self.no_variant = no_variant
        self.default = default

    def compose(self):
        with Container():
            yield Label(self.text)
            with Horizontal():
                yield Button(self.yes_text, id = 'yes', variant = self.yes_variant)
                yield Button(self.no_text, id = 'no', variant = self.no_variant)

    def on_mount(self):
        if self.default:
            self.query_one(f'#{self.default}').focus()

    @on(Button.Pressed, '#yes')
    def yes(self):
        self.dismiss(True)

    @on(Button.Pressed, '#no')
    def no(self):
        self.dismiss(False)