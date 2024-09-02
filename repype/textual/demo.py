import tempfile

from textual.app import App

from repype.typing import Callable


class Demo:
    """
    Helper class to control the `app` interactively (e.g., within a Jupyter notebook).

    Probably only useful for demonstration purposes.
    """

    def __init__(self, app: App, **kwargs):
        self.app = app
        self.ctx = app.run_test(**kwargs)
        self.pilot = None

    def screenshot(self) -> None:
        """
        Capture a screenshot of the current state of the `app`.

        The screenshot is embedded in the Jupyter notebook.
        """
        with tempfile.NamedTemporaryFile() as file:
            self.app.save_screenshot(file.name)

    async def press(self, *keys) -> None:
        """
        Press the given `keys` in the `app`.
        """
        await self.pilot.press(*keys)
        await self.pilot.pause(0)

    async def wait(self, seconds) -> None:
        """
        Wait for the number of `seconds`.
        """
        await self.pilot.pause(seconds)

    async def wait_for_condition(self, condition: Callable[[], bool]) -> None:
        """
        Wait until the given `condition` is met (polled once per second).
        """
        while not condition():
            await self.wait(1)


async def run(app: App, **kwargs) -> Demo:
    """
    Co-routine that runs the `app`` and returns the :class:`Demo` object.
    """
    demo = Demo(app, **kwargs)
    demo.pilot = await demo.ctx.__aenter__()
    await demo.pilot.pause(0)
    return demo
