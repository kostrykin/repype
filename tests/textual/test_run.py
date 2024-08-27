import asyncio
import traceback
import types
import unittest.mock

import repype.status
import repype.textual.app
import repype.textual.run


test_case = 'tests.test_textual.TextualTestCase'


async def test__success(test_case):
    async with test_case.app.run_test() as pilot:

        # Configure the `RunScreen` with a mocked `RunContext` object
        ctx1 = unittest.mock.MagicMock()
        ctx1.task.path.__str__.return_value = 'task1'
        ctx1.task.path.resolve.return_value = '/path/to/task1'
        screen = repype.textual.run.RunScreen([ctx1])

        with unittest.mock.patch.object(test_case.app, 'batch') as mock_batch:

            # Configure the mock batch
            async def batch_run(self, contexts, status):
                try:
                    test_case.assertEqual(contexts, [ctx1])

                    await asyncio.sleep(1)
                    collapsible = screen.query_one('#run-task-1')
                    container = screen.query_one('#run-task-1-container')
                    test_case.assertTrue(collapsible.collapsed)
                    test_case.assertEqual(collapsible.title, ctx1.task.path.resolve())
                    test_case.assertEqual(len(container.children), 0)

                    repype.status.update(status, info = 'enter', task = '/path/to/task1')

                    await asyncio.sleep(1)
                    test_case.assertFalse(collapsible.collapsed)
                    test_case.assertEqual(len(container.children), 1)
                    test_case.assertIsInstance(container.children[0], repype.textual.run.Label)
                    test_case.assertEqual(str(container.children[0].renderable), '')  # FIXME: Why is there an empty label?

                    repype.status.update(status, 'update 1')

                    await asyncio.sleep(1)
                    test_case.assertEqual(len(container.children), 2)
                    test_case.assertIsInstance(container.children[1], repype.textual.run.Label)
                    test_case.assertEqual(str(container.children[1].renderable), 'update 1')

                    return True
                
                except:
                    print(traceback.format_exc())
                    raise

                finally:
                    mock_batch.task_process = None

            mock_batch.task_process = 1
            mock_batch.run = types.MethodType(batch_run, mock_batch)

            # Push the `RunScreen` and wait for the batch to complete
            await test_case.app.push_screen(screen)
            while mock_batch.task_process:
                await asyncio.sleep(1)
            test_case.assertTrue(screen.success)