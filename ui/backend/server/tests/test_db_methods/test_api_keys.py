import pytest
from trackingserver_base.auth.api_keys import create_api_key_for_user, validate_api_key

from tests.test_db_methods.utils import setup_sample_user_random


@pytest.mark.asyncio
async def test_create_api_key_for_user(db):
    user = await setup_sample_user_random()
    api_key, obj = await create_api_key_for_user(user, "foo_key")
    assert len(api_key) > 64
    return user, api_key


@pytest.mark.asyncio
async def test_validate_api_key_valid(db):
    user, api_key = await test_create_api_key_for_user(db)
    assert await validate_api_key(user.email, api_key)


@pytest.mark.asyncio
async def test_validate_api_key_invalid(db):
    user, api_key = await test_create_api_key_for_user(db)
    assert await validate_api_key(user.email, str(reversed(api_key))) is False
    assert await validate_api_key(user.email, "invalid_key") is False
    assert await validate_api_key(user.email, "very_long_invalid_key" * 100000) is False
