import repype.textual.batch


test_case = 'tests.test_textual.TextualTestCase'


async def run(test_case):
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
        test_case.assertIn(str(task1_node.data.path), task1_node.label)
        test_case.assertIn('sigma=2', task2_node.label)
        test_case.assertIn('pending', task1_node.label)
        test_case.assertIn('pending', task2_node.label)