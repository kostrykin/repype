test_case = 'tests.test_textual.TextualTestCase'


async def run(test_case):
    async with test_case.app.run_test() as pilot:

        assert test_case.app.is_running