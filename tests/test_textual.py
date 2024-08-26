import unittest

import repype.textual.app
import repype.textual.batch
import repype.textual.confirm
import repype.textual.editor
import repype.textual.run
from . import test_repype


class Repype(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.repype_segmentation = test_repype.repype_segmentation()
        self.repype_segmentation.setUp()
        self.app = repype.textual.app.Repype(path = self.repype_segmentation.root_path)

    async def asyncTearDown(self):
        self.repype_segmentation.tearDown()

    async def test_screen(self):
        async with self.app.run_test() as pilot:
            pass
            #print(self.app.screen)
            #await pilot.press('q')

    async def test_quit(self):
        async with self.app.run_test() as pilot:
            pass
            #await pilot.press('q')
            #self.assertFalse(self.app.is_running)