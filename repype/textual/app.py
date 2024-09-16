import pathlib

from textual.app import App
from textual.binding import Binding

import repype.batch
from repype.typing import (
    Optional,
    PathLike,
)

from .batch import BatchScreen

# Unbind `CTRL+C`
App.BINDINGS = []


class Repype(App):
    """
    The main application class. Can only be instantiated once per process.

    Arguments:
        path: The root directory for batch processing, where the tasks are loaded from. Passed through to
            :meth:`repype.batch.Batch.load`. Parsed from the command-line, if `None`.
        headless: Run in headless mode for debugging. If `True`, overwrites the `headless` argument passed to
            :meth:`run`. Can also be overwritten by the command-line argument ``--headless``.
        **kwargs: Additional keyword arguments passed to :class:`repype.batch.Batch`.
    """

    BINDINGS = [
        Binding('q', 'exit', 'Exit'),
    ]
    """
    The bindings of the application.
    """

    SCREENS = {'batch': BatchScreen()}
    """
    Initially available screens.
    """

    ENABLE_COMMAND_PALETTE = False
    """
    Disable the command palette.
    """

    CSS_PATH = 'repype.tcss'
    """
    The path to the stylesheet file.
    """

    headless: bool
    """
    If `True`, overwrites the `headless` argument passed to :meth:`run`.
    """

    path: pathlib.Path
    """
    The root directory for batch processing, where the tasks are loaded from.
    """

    batch: repype.batch.Batch
    """
    The batch object.
    """

    def __init__(self, path: Optional[PathLike] = None, headless: bool = False, **kwargs):
        if path is None:
            import argparse
            parser = argparse.ArgumentParser()
            parser.add_argument('path', help='Root directory for batch processing.')
            parser.add_argument('--headless', action='store_true', help='Run in headless mode for debugging.')
            args = parser.parse_args()
            path = args.path
            if args.headless:
                headless = True

        self.headless = headless
        self.batch = repype.batch.Batch(**kwargs)
        self.path = pathlib.Path(path)
        super().__init__()

    def run(self, *args, headless: bool = False, **kwargs) -> None:
        headless = headless or self.headless
        super().run(*args, headless = headless, **kwargs)

    def on_mount(self) -> None:
        """
        Loads the :class:`.BatchScreen` as the initial screen of the app.
        """
        self.title = 'repype'
        self.push_screen('batch')

    def action_exit(self) -> None:
        """
        Exits the app.
        """
        self.exit()
