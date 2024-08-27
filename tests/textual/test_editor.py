import repype.textual.editor
from tests.textual.test_batch import find_tree_node_by_task
import textual.css.query
import unittest.mock


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
        screen = test_case.app.screen
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
        screen = test_case.app.screen
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
        test_case.assertIsInstance(test_case.app.screen, repype.textual.editor.ConfirmScreen)
        test_case.app.screen.yes()
        await pilot.pause(0)
        mock_EditorScreen_dismiss.assert_called_once_with(False)