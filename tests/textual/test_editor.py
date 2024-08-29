import unittest.mock

import repype.textual.confirm
import repype.textual.editor
import textual.css.query


test_case = 'tests.test_textual.TextualTestCase'


async def test__new(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    ctx1 = batch.context(test_case.root_path / 'task')

    # Run the app and push the editor screen
    async with test_case.app.run_test() as pilot:

        screen = repype.textual.editor.EditorScreen.new(parent_task = ctx1.task)
        await test_case.app.push_screen(screen)

        # Verify the editor screen
        test_case.assertEqual(screen.parent_task, ctx1.task)
        test_case.assertIsNone(screen.my_task)
        test_case.assertEqual(screen.mode, 'new')
        test_case.assertIn('Parent task:', screen.query_one('#editor-main-header').renderable)
        test_case.assertIn(str(ctx1.task.path), screen.query_one('#editor-main-header').renderable)
        test_case.assertEqual(screen.query_one('#editor-main-name').value, '')
        test_case.assertEqual(screen.query_one('#editor-code').text, '')


async def test__edit(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    ctx1 = batch.context(test_case.root_path / 'task')

    # Run the app and push the editor screen
    async with test_case.app.run_test() as pilot:

        screen = repype.textual.editor.EditorScreen.edit(task = ctx1.task)
        await test_case.app.push_screen(screen)

        # Verify the editor screen
        test_case.assertEqual(screen.my_task, ctx1.task)
        test_case.assertIsNone(screen.parent_task)
        test_case.assertEqual(screen.mode, 'edit')
        test_case.assertIn('Task:', screen.query_one('#editor-main-header').renderable)
        test_case.assertIn(str(ctx1.task.path), screen.query_one('#editor-main-header').renderable)
        with test_case.assertRaises(textual.css.query.NoMatches):
            screen.query_one('#editor-main-name')
        test_case.assertEqual(
            screen.query_one('#editor-code').text,
            (ctx1.task.path / 'task.yml').read_text(),
        )


@unittest.mock.patch('repype.textual.editor.EditorScreen.dismiss')
async def test__action_cancel(test_case, mock_EditorScreen_dismiss):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    ctx1 = batch.context(test_case.root_path / 'task')

    # Run the app and push the editor screen
    async with test_case.app.run_test() as pilot:

        screen = repype.textual.editor.EditorScreen.edit(task = ctx1.task)
        await test_case.app.push_screen(screen)

        # Close the editor without saving
        await pilot.press('ctrl+c')

        # Confirm
        test_case.assertIsInstance(test_case.app.screen, repype.textual.confirm.ConfirmScreen)
        test_case.app.screen.yes()

        # Verify the editor screen was dismissed
        await pilot.pause(0)
        mock_EditorScreen_dismiss.assert_called_once_with(False)


@unittest.mock.patch('repype.textual.editor.EditorScreen.dismiss')
async def test__action_save__new(test_case, mock_EditorScreen_dismiss):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    task1 = batch.task(test_case.root_path / 'task')

    # Run the app and push the editor screen
    async with test_case.app.run_test() as pilot:

        screen = repype.textual.editor.EditorScreen.new(parent_task = task1)
        await test_case.app.push_screen(screen)

        # Set the task name and specification
        await pilot.press(*list('task3'))
        await pilot.press('tab')
        await pilot.press(*list('runnable: true'))

        # Save and close the editor
        await pilot.press('ctrl+s')

        # Verify the editor screen was dismissed
        mock_EditorScreen_dismiss.assert_called_once_with(True)

        # Verify the new task was created
        spec = (task1.path / 'task3' / 'task.yml').read_text()
        test_case.assertEqual(spec, 'runnable: true')


@unittest.mock.patch('repype.textual.editor.EditorScreen.dismiss')
async def test__action_save__new__error(test_case, mock_EditorScreen_dismiss):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    task1 = batch.task(test_case.root_path / 'task')

    # Run the app and push the editor screen
    async with test_case.app.run_test() as pilot:

        inputs = [
            dict(name = '', spec = ''),
            dict(name = 'task3', spec = ''),
            dict(name = 'task3', spec = ' '),
            dict(name = 'task3', spec = '*:*'),
            dict(name = '', spec = 'runnable: true'),
            dict(name = ' ', spec = 'runnable: true'),
        ]

        for input in inputs:
            screen = repype.textual.editor.EditorScreen.new(parent_task = task1)
            await test_case.app.push_screen(screen)

            # Set the task name and specification
            await pilot.press(*list(input['name']))
            await pilot.press('tab')
            await pilot.press(*list(input['spec']))

            # Save and close the editor
            await pilot.press('ctrl+s')

            # Verify the editor screen was not dismissed
            mock_EditorScreen_dismiss.assert_not_called()

            # Verify that the task was not created
            test_case.assertFalse((task1.path / 'task3').exists())

            # Reset the screen for the next set of inputs
            test_case.app.pop_screen()


@unittest.mock.patch('repype.textual.editor.EditorScreen.dismiss')
async def test__action_save__edit(test_case, mock_EditorScreen_dismiss):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    task2 = batch.task(test_case.root_path / 'task' / 'sigma=2')

    # Run the app and push the editor screen
    async with test_case.app.run_test() as pilot:

        screen = repype.textual.editor.EditorScreen.edit(task = task2)
        await test_case.app.push_screen(screen)

        # Update the specification
        screen.task_spec_editor.text = 'runnable: true'

        # Save and close the editor
        await pilot.press('ctrl+s')

        # Verify the editor screen was dismissed
        mock_EditorScreen_dismiss.assert_called_once_with(True)

        # Verify the task was updated
        spec = (task2.path / 'task.yml').read_text()
        test_case.assertEqual(spec, 'runnable: true')


@unittest.mock.patch('repype.textual.editor.EditorScreen.dismiss')
async def test__action_save__edit__error(test_case, mock_EditorScreen_dismiss):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    task2 = batch.task(test_case.root_path / 'task' / 'sigma=2')
    original_spec = (task2.path / 'task.yml').read_text()

    # Run the app and push the editor screen
    async with test_case.app.run_test() as pilot:

        inputs = [
            dict(spec = ''),
            dict(spec = ' '),
            dict(spec = '*:*'),
        ]

        for input in inputs:
            screen = repype.textual.editor.EditorScreen.edit(task = task2)
            await test_case.app.push_screen(screen)

            # Update the specification
            screen.task_spec_editor.text = input['spec']

            # Save and close the editor
            await pilot.press('ctrl+s')

            # Verify the editor screen was not dismissed
            mock_EditorScreen_dismiss.assert_not_called()

            # Verify that the task was not updated
            spec = (task2.path / 'task.yml').read_text()
            test_case.assertEqual(spec, original_spec)

            # Reset the screen for the next set of inputs
            test_case.app.pop_screen()