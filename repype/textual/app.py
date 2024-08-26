import pathlib

import repype.batch
from textual.app import (
    App,
)
from textual.binding import (
    Binding,
)
from .batch import BatchScreen


# Unbind CTRL+C
App.BINDINGS = []


class Repype(App):

    BINDINGS = [
        Binding('q', 'exit', 'Exit'),
    ]
    SCREENS = {'batch': BatchScreen()}
    ENABLE_COMMAND_PALETTE = False
    CSS_PATH = '../../repype.tcss'

    def __init__(self, path = None, headless = False):
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
        self.batch = repype.batch.Batch()
        self.path = pathlib.Path(path)
        super().__init__()

    def run(self, *args, headless = False, **kwargs):
        headless = headless or self.headless
        super().run(*args, headless = headless, **kwargs)

    def on_mount(self):
        self.title = 'repype'
        self.push_screen('batch')

    def action_exit(self):
        self.exit()