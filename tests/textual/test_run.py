import asyncio
import pathlib
import traceback
import types
import unittest.mock

import repype.status
import repype.textual.confirm
import repype.textual.app
import repype.textual.run


test_case = 'tests.test_textual.TextualTestCase'


@unittest.mock.patch.object(repype.textual.run, 'log')
async def test__success(test_case, mock_log):
    async with test_case.app.run_test() as pilot:

        # Configure the `RunScreen` with a mocked `RunContext` object
        ctx1 = unittest.mock.MagicMock()
        ctx1.task.path.__str__.return_value = 'task1'
        ctx1.task.path.resolve.return_value = pathlib.Path('/path/to/task1')
        screen = repype.textual.run.RunScreen([ctx1])

        with unittest.mock.patch.object(test_case.app, 'batch') as mock_batch:

            # Configure the mock batch
            async def batch_run(self, contexts, status):
                try:
                    test_case.assertEqual(contexts, [ctx1])

                    await asyncio.sleep(1)
                    task_ui = screen.task_ui(ctx1.task.path)
                    test_case.assertTrue(task_ui.collapsible.collapsed)
                    test_case.assertEqual(task_ui.collapsible.title, str(ctx1.task.path.resolve()))
                    test_case.assertEqual(len(task_ui.container.children), 0)
                    test_case.assertIsNone(screen.current_task_path)

                    # Test `enter` status update

                    repype.status.update(status, info = 'enter', task = '/path/to/task1')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(screen.current_task_path, ctx1.task.path.resolve())
                    test_case.assertFalse(task_ui.collapsible.collapsed)
                    test_case.assertEqual(len(task_ui.container.children), 1)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), '')  # FIXME: Why is there an empty label?

                    # Test plain status update

                    repype.status.update(status, 'update 1')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 2)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), 'update 1')

                    # Test intermediate status update

                    repype.status.update(status, 'intermediate 1', intermediate = True)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'block')
                    test_case.assertEqual(len(task_ui.container.children), 2)
                    test_case.assertEqual(str(task_ui.intermediate_label.renderable), 'intermediate 1')
                    test_case.assertEqual(task_ui.intermediate_progressbar.progress, 0)
                    test_case.assertIsNone(task_ui.intermediate_progressbar.total)

                    # Test intermediate status clearance

                    repype.status.update(status, None, intermediate = True)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 2)

                    # Test two subsequent intermediate status updates

                    repype.status.update(status, 'intermediate 2', intermediate = True)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'block')
                    test_case.assertEqual(len(task_ui.container.children), 2)
                    test_case.assertEqual(str(task_ui.intermediate_label.renderable), 'intermediate 2')
                    test_case.assertEqual(task_ui.intermediate_progressbar.progress, 0)
                    test_case.assertIsNone(task_ui.intermediate_progressbar.total)

                    repype.status.update(status, 'intermediate 3', intermediate = True)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'block')
                    test_case.assertEqual(len(task_ui.container.children), 2)
                    test_case.assertEqual(str(task_ui.intermediate_label.renderable), 'intermediate 3')
                    test_case.assertEqual(task_ui.intermediate_progressbar.progress, 0)
                    test_case.assertIsNone(task_ui.intermediate_progressbar.total)

                    # Test permanent status update after intermediate status

                    repype.status.update(status, 'update 2')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 3)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), 'update 2')

                    # Test `start` status update

                    repype.status.update(status, info = 'start', pickup = None, first_stage = None)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 4)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), 'Starting from scratch')

                    repype.status.update(status, info = 'start', pickup = '/some/task', first_stage = None)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 5)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'Picking up from: /some/task (copy)')

                    repype.status.update(status, info = 'start', pickup = '/some/task', first_stage = 'some-stage')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 6)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'Picking up from: /some/task (some-stage)')

                    # Test `process` status update

                    repype.status.update(status, info = 'process', step = 0, step_count = 1, input_id = 'input-1')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 7)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'(1/1) Processing: input-1')  # FIXME: How to check the `[bold]...[/bold]` tags?

                    # Test `start-stage` status update

                    repype.status.update(status, info = 'start-stage', stage = 'some-stage')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 8)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'Starting stage: some-stage')

                    # Test `storing` status update

                    repype.status.update(status, info = 'storing')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 9)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'Storing results...')

                    # Test `completed` status update

                    repype.status.update(status, info = 'completed', task = '/path/to/task1')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 10)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'Results have been stored')
                    test_case.assertEqual(task_ui.collapsible.title, f'{ctx1.task.path.resolve()} (done)')

                    # Test `error` status update

                    repype.status.update(status, info = 'error', traceback = 'the traceback')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 12)
                    test_case.assertIsInstance(task_ui.container.children[-2], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-2].renderable), f'An error occurred:')
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'the traceback')

                    # Test `interrupted` status update

                    repype.status.update(status, info = 'interrupted')

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'none')
                    test_case.assertEqual(len(task_ui.container.children), 13)
                    test_case.assertIsInstance(task_ui.container.children[-1], repype.textual.run.Label)
                    test_case.assertEqual(str(task_ui.container.children[-1].renderable), f'Batch run interrupted')

                    # Test `progress` status update

                    repype.status.update(status, info = 'progress', details = 'the details', step = 0, max_steps = 2, intermediate = True)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'block')
                    test_case.assertEqual(len(task_ui.container.children), 13)
                    test_case.assertEqual(str(task_ui.intermediate_label.renderable), 'the details')
                    test_case.assertEqual(task_ui.intermediate_progressbar.progress, 0)
                    test_case.assertEqual(task_ui.intermediate_progressbar.total, 2)

                    repype.status.update(status, info = 'progress', details = 'the details', step = 1, max_steps = 2, intermediate = True)

                    await asyncio.sleep(1)
                    test_case.assertEqual(task_ui.intermediate.styles.display, 'block')
                    test_case.assertEqual(len(task_ui.container.children), 13)
                    test_case.assertEqual(str(task_ui.intermediate_label.renderable), 'the details')
                    test_case.assertEqual(task_ui.intermediate_progressbar.progress, 1)
                    test_case.assertEqual(task_ui.intermediate_progressbar.total, 2)

                    return True
                
                except:
                    print(traceback.format_exc())
                    raise

                finally:
                    mock_batch.task_process = None

            mock_batch.task.return_value = ctx1.task
            mock_batch.task_process = 1
            mock_batch.run = types.MethodType(batch_run, mock_batch)

            # Push the `RunScreen` and wait for the batch to complete
            await test_case.app.push_screen(screen)
            while mock_batch.task_process:
                await asyncio.sleep(1)
            test_case.assertTrue(screen.success)
            test_case.assertIsNone(screen.current_task_path)


async def test__action_cancel(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    ctx1 = batch.context(test_case.root_path / 'task')

    # Verify the `RunScreen` and its contents
    async with test_case.app.run_test() as pilot:

        # Configure the `RunScreen` with a mocked `RunContext` object
        screen = repype.textual.run.RunScreen([ctx1])
        with unittest.mock.patch.object(screen, 'handle_new_status') as mock_handle_new_status:
            await test_case.app.push_screen(screen)

            # Cancel the batch run
            await pilot.pause(0.2)
            await pilot.press('ctrl+c')

            # Confirm
            test_case.assertIsInstance(test_case.app.screen, repype.textual.confirm.ConfirmScreen)
            test_case.app.screen.yes()
            test_case.assertIsInstance(test_case.app.screen, repype.textual.run.RunScreen)
            await pilot.pause(0)

            # Wait for the batch run to be cancelled
            if test_case.app.batch.task_process:
                await test_case.app.batch.task_process.wait()
            await pilot.pause(1)

        # Verify the results
        test_case.assertFalse(screen.success)
        test_case.assertIsNone(screen.current_task_path)
        test_case.assertFalse(mock_handle_new_status.call_args_list[-1].kwargs['intermediate'])
        test_case.assertEqual(
            mock_handle_new_status.call_args_list[-1].kwargs['status'],
            dict(
                info = 'interrupted',
                exit_code = None,
            ),
        )


@unittest.mock.patch.object(repype.textual.run, 'log')
async def test__action_close__while_running(test_case, mock_log):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    ctx1 = batch.context(test_case.root_path / 'task')

    # Verify the `RunScreen` and its contents
    async with test_case.app.run_test() as pilot:

        # Configure the `RunScreen` with a mocked `RunContext` object
        screen = repype.textual.run.RunScreen([ctx1])
        await test_case.app.push_screen(screen)
        await pilot.pause(0)

        # Trigger `action_close`
        await pilot.press('escape')

        # Verify that nothing happened
        test_case.assertIsInstance(test_case.app.screen, repype.textual.run.RunScreen)

        # Finish the batch run
        await test_case.app.batch.task_process.wait()


@unittest.mock.patch.object(repype.textual.run, 'log')
@unittest.mock.patch.object(repype.textual.run.RunScreen, 'dismiss', autospec = True)
async def test__action_close(test_case, mock_dismiss, mock_log):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    ctx1 = batch.context(test_case.root_path / 'task')

    # Verify the `RunScreen` and its contents
    async with test_case.app.run_test() as pilot:

        # Configure the `RunScreen` with a mocked `RunContext` object
        screen = repype.textual.run.RunScreen([ctx1])
        await test_case.app.push_screen(screen)
        await pilot.pause(0)

        # Finish the batch run
        await test_case.app.batch.task_process.wait()

        # Trigger `action_close`
        await pilot.press('escape')

        # Verify that the screen was closed
        mock_dismiss.assert_called_once_with(screen, True)