test_case = 'tests.test_textual.TextualTestCase'


async def test(test_case):
    async with test_case.app.run_test() as pilot:

        assert test_case.app.is_running


async def test__action_exit(test_case):
    async with test_case.app.run_test() as pilot:

        await pilot.press('q')
        assert not test_case.app.is_running