import repype.batch
import repype.textual.batch


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
    ctx1 = batch.context(test_case.root_path / 'task')
    ctx2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    ctx2.run()

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
        test_case.assertEqual(task1_node.data, ctx1.task)
        test_case.assertEqual(task2_node.data, ctx2.task)
        test_case.assertIn(str(ctx1.task.path), task1_node.label)
        test_case.assertIn('sigma=2', task2_node.label)
        test_case.assertIn('pending', task1_node.label)
        test_case.assertNotIn('pending', task2_node.label)


async def test__action_delete_task__none(test_case):

    # Load the batch
    batch = repype.batch.Batch()
    batch.load(test_case.root_path)

    # Load the tasks
    ctx1 = batch.context(test_case.root_path / 'task')
    ctx2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    ctx2.run()

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
    ctx1 = batch.context(test_case.root_path / 'task')
    ctx2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    ctx2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the second task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task1_node = find_tree_node_by_task(task_tree.root, ctx1.task)
        task_tree.select_node(task1_node)

        # Delete the selected task
        await pilot.press('d')

        # Confirm
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.ConfirmScreen)
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
    ctx1 = batch.context(test_case.root_path / 'task')
    ctx2 = batch.context(test_case.root_path / 'task' / 'sigma=2')

    # Mark `task2` as completed
    ctx2.run()

    # Verify the batch screen and its contents
    async with test_case.app.run_test() as pilot:

        # Select the second task
        task_tree = test_case.app.screen.query_one('#setup-tasks')
        task2_node = find_tree_node_by_task(task_tree.root, ctx2.task)
        task_tree.select_node(task2_node)

        # Delete the selected task
        await pilot.press('d')

        # Confirm
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.ConfirmScreen)
        test_case.app.screen.yes()
        test_case.assertIsInstance(test_case.app.screen, repype.textual.batch.BatchScreen)
        await pilot.pause(0)

        # Verify tree structure
        test_case.assertEqual(len(task_tree.root.children), 1)
        test_case.assertEqual(len(task_tree.root.children[0].children), 0)

        # Verify task nodes
        task1_node = task_tree.root.children[0]
        test_case.assertEqual(task1_node.data, ctx1.task)