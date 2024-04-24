import asyncio

import pytest
from trackingserver_base.auth.testing import TEST_USERS, TestAPIAuthenticator


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    # yield loop
    loop.close()


@pytest.fixture(scope="session")
def django_db_setup(django_db_blocker, event_loop):
    with django_db_blocker.unblock():
        for username in TEST_USERS:
            event_loop.run_until_complete(TestAPIAuthenticator.ensure(username))
