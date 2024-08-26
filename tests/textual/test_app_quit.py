test_case = 'tests.test_textual.TextualTestCase'


async def run(test_case):
    async with test_case.app.run_test() as pilot:

        await pilot.press('q')
        assert not test_case.app.is_running