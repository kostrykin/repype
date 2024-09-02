import unittest.mock

import repype.batch
import repype.textual.batch
import repype.textual.confirm

test_case = 'tests.test_textual.TextualTestCase'


def find_tree_node_by_task(node, task):
    if node.data and node.data == task:
        return node
    else:
        for child in node.children:
            result = find_tree_node_by_task(child, task)
            if result:
                return result
        return None


async def test(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    rc2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:
        
        # Verify screen
        screen = test_case.app.screen
        test_case.assertIsInstance(screen, repype.textual.batch.BatchScreen)

        # Verify tree widget
        task_tree = screen.query_one('#setup-tasks')
        test_case.assertIsInstance(task_tree, repype.textual.batch.Tree)

        # Verify tree structure
        test_case.assertEqual(len(task_tree.root.children), 1)
        test_case.assertEqual(len(task_tree.root.children[0].children), 1)
        test_case.assertEqual(len(task_tree.root.children[0].children[0].children), 0)

        # Verify task nodes
        task1_node = task_tree.root.children[0]
        task2_node = task_tree.root.children[0].children[0]
        test_case.assertEqual(task1_node.data, rc1.task)
        test_case.assertEqual(task2_node.data, rc2.task)
        test_case.assertIn(str(rc1.task.path), task1_node.label)
        test_case.assertIn('sigma=2', task2_node.label)
        test_case.assertIn('pending', task1_node.label)
        test_case.assertNotIn('pending', task2_node.label)

        # Verify the `pending` label
        test_case.assertIn('Tasks:', screen.query_one(f'#batch-summary').renderable)
        test_case.assertIn('1 pending', screen.query_one(f'#batch-summary').renderable)
        test_case.assertIn('0 queued', screen.query_one(f'#batch-summary').renderable)


async def test__action_delete_task__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    rc2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task_tree.select_node(task_tree.root)

        # Delete the selected task
        await pilot.press('d')

        # Verify that nothing happened
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)


async def test__action_delete_task__task1(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    rc2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the second task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)

        # Delete the selected task
        await pilot.press('d')

        # Confirm
        test_case.assertIsInstance(test_case.app.screen, repype.textual.confirm.ConfirmScreen)
        test_case.app.screen.yes()
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)
        await pilot.pause(0)

        # Verify tree structure
        test_case.assertEqual(len(task_tree.root.children), 0)


async def test__action_delete_task__task2(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    rc2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the second task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task2_node = find_tree_node_by_task(task_tree.root, rc2.task)
        task_tree.select_node(task2_node)

        # Delete the selected task
        await pilot.press('d')

        # Confirm
        test_case.assertIsInstance(test_case.app.screen, repype.textual.confirm.ConfirmScreen)
        test_case.app.screen.yes()
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)
        await pilot.pause(0)

        # Verify tree structure
        test_case.assertEqual(len(task_tree.root.children), 1)
        test_case.assertEqual(len(task_tree.root.children[0].children), 0)

        # Verify task nodes
        task1_node = task_tree.root.children[0]
        test_case.assertEqual(task1_node.data, rc1.task)


async def test__reset_task__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task1` and `task2` as completed
    rc1.run()
    rc2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task_tree.select_node(task_tree.root)

        # Delete the selected task
        await pilot.press('R')

        # Verify that nothing happened
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)


async def test__reset_task__task1(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task1` and `task2` as completed
    rc1.run()
    rc2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the first task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)

        # Delete the selected task
        await pilot.press('R')

        # Confirm
        test_case.assertIsInstance(test_case.app.screen, repype.textual.confirm.ConfirmScreen)
        test_case.app.screen.yes()
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)
        await pilot.pause(0)

        # Verify task nodes
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task2_node = find_tree_node_by_task(task_tree.root, rc2.task)
        test_case.assertIn('pending', task1_node.label)
        test_case.assertNotIn('pending', task2_node.label)


async def test__add_task__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task_tree.select_node(task_tree.root)

        # Add sub-task below selected task (none)
        await pilot.press('a')

        # Verify that nothing happened
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)


@unittest.mock.patch('repype.textual.editor.EditorScreen.new', new_callable = unittest.mock.AsyncMock)
async def test__add_task__task1(test_case, mock_EditorScreen_new):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the first task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)

        # Patch `BatchScreen.update_task_tree` method
        with unittest.mock.patch.object(test_case.app.screen, 'update_task_tree') as mock_update_task_tree:

            # Test with `EditorScreen.new` returning `False`
            mock_EditorScreen_new.return_value = False
            await pilot.press('a')
            mock_EditorScreen_new.assert_awaited_once_with(test_case.app, parent_task = rc1.task)
            mock_update_task_tree.assert_not_called()

            # Reset the mocks
            mock_EditorScreen_new.reset_mock()
            mock_update_task_tree.reset_mock()

            # Test with `EditorScreen.new` returning `True`
            mock_EditorScreen_new.return_value = True
            await pilot.press('a')
            mock_EditorScreen_new.assert_awaited_once_with(test_case.app, parent_task = rc1.task)
            mock_update_task_tree.assert_called_once()


async def test__edit_task__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task_tree.select_node(task_tree.root)

        # Edit the selected task (none)
        await pilot.press('e')

        # Verify that nothing happened
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)


@unittest.mock.patch('repype.textual.editor.EditorScreen.edit', new_callable = unittest.mock.AsyncMock)
async def test__edit_task__task1(test_case, mock_EditorScreen_edit):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the first task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)

        # Patch `BatchScreen.update_task_tree` method
        with unittest.mock.patch.object(test_case.app.screen, 'update_task_tree') as mock_update_task_tree:

            # Test with `EditorScreen.edit` returning `False`
            mock_EditorScreen_edit.return_value = False
            await pilot.press('e')
            mock_EditorScreen_edit.assert_awaited_once_with(test_case.app, task = rc1.task)
            mock_update_task_tree.assert_not_called()

            # Reset the mocks
            mock_EditorScreen_edit.reset_mock()
            mock_update_task_tree.reset_mock()

            # Test with `EditorScreen.edit` returning `True`
            mock_EditorScreen_edit.return_value = True
            await pilot.press('e')
            mock_EditorScreen_edit.assert_awaited_once_with(test_case.app, task = rc1.task)
            mock_update_task_tree.assert_called_once()


async def test__run_task__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task_tree.select_node(task_tree.root)

        # Run the selected task (none)
        await pilot.press('r')

        # Verify that nothing happened
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)


async def test__run_task__completed(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')

    # Mark `task1` as completed
    rc1.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the first task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)

        # Run the selected task
        await pilot.press('r')

        # Verify that nothing happened
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)


@unittest.mock.patch('repype.textual.batch.RunScreen')
@unittest.mock.patch.object(repype.textual.batch.BatchScreen, 'confirm', return_value = True)
async def test__run_task__pending(test_case, mock_confirm, mock_RunScreen):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the first task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)

        # Patch `app.push_screen_wait` method
        with unittest.mock.patch.object(test_case.app, 'push_screen_wait') as mock_push_screen_wait:

            # Patch `BatchScreen.update_task_tree` method
            with unittest.mock.patch.object(test_case.app.screen, 'update_task_tree') as mock_update_task_tree:

                # Test with `mock_push_screen_wait` returning 0
                mock_push_screen_wait.return_value = 0
                await pilot.press('r')

                # Perform the checks
                mock_RunScreen.assert_called_once_with([rc1])
                mock_update_task_tree.assert_not_called()

                # Reset the mocks
                mock_RunScreen.reset_mock()
                mock_push_screen_wait.reset_mock()
                mock_update_task_tree.reset_mock()

                # Test with `mock_push_screen_wait` returning 1
                mock_push_screen_wait.return_value = 1
                await pilot.press('r')

                # Perform the checks
                mock_RunScreen.assert_called_once_with([rc1])
                mock_update_task_tree.assert_called_once()


async def test__toggle_task__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task_tree.select_node(task_tree.root)
        label = str(task_tree.cursor_node.label)

        # Toggle selected task (none)
        await pilot.press('x')

        # Verify that nothing happened
        test_case.assertEqual(str(task_tree.cursor_node.label), label)


async def test__toggle_task__task1(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)
        label = str(task_tree.cursor_node.label)[4:]

        # Toggle selected task (none)
        await pilot.press('x')

        # Verify that nothing happened
        test_case.assertEqual(str(task_tree.cursor_node.label), '[x] ' + label)

        # Toggle selected task (none)
        await pilot.press('x')

        # Verify that nothing happened
        test_case.assertEqual(str(task_tree.cursor_node.label), '[ ] ' + label)


async def test__non_pending_tasks__task1(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task1` as completed
    rc1.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        test_case.assertEqual(
            frozenset(test_case.app.screen.non_pending_tasks),
            frozenset(
                [
                    rc1.task,
                ],
            ),
        )


async def test__non_pending_tasks__all(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task1` and `task2` as completed
    rc1.run()
    rc2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        test_case.assertEqual(
            frozenset(test_case.app.screen.non_pending_tasks),
            frozenset(
                [
                    rc1.task,
                    rc2.task,
                ],
            ),
        )


async def test__non_pending_tasks__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        test_case.assertEqual(
            frozenset(test_case.app.screen.non_pending_tasks),
            frozenset(),
        )


async def test__toggle_task__completed(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')

    # Mark `task1` as completed
    rc1.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the root node (not a task)
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, rc1.task)
        task_tree.select_node(task1_node)
        label = str(task_tree.cursor_node.label)

        # Toggle selected task (none)
        await pilot.press('x')

        # Verify that nothing happened
        test_case.assertEqual(str(task_tree.cursor_node.label), label)


async def test__update_task_tree(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    rc1 = batch.context(test_case.root_path / 'task')
    rc2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Wait for the screen to finish loading
        await pilot.pause(0)

        # Mark both tasks as queued
        test_case.app.screen.queued_tasks = [
            rc1.task,
            rc2.task,
        ]

        # Mark `task1` as completed
        rc1.run()

        # Call the `update_task_tree` method
        test_case.app.screen.update_task_tree()

        # Test the results
        test_case.assertEqual(
            test_case.app.screen.queued_tasks,
            [rc2.task]
        )
        label1 = str(find_tree_node_by_task(test_case.app.screen.task_tree.root, rc1.task).label)
        label2 = str(find_tree_node_by_task(test_case.app.screen.task_tree.root, rc2.task).label)
        test_case.assertTrue(label1.startswith('[ ] '))
        test_case.assertTrue(label2.startswith('[x] '))
        test_case.assertTrue('pending' not in label1)
        test_case.assertTrue('pending' in label2)