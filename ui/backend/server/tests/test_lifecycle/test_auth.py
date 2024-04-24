import uuid

import pytest
from django.test import AsyncClient


@pytest.mark.asyncio
async def test_phone_home(async_client: AsyncClient, db):
    response = await async_client.get(
        "/api/v1/phone_home", headers={"test_username": "user_individual@no_team.com"}
    )
    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "You are authenticated!"}


@pytest.mark.asyncio
async def test_whoami_no_team(async_client: AsyncClient, db):
    response = await async_client.get(
        "/api/v1/whoami", headers={"test_username": "user_individual@no_team.com"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["user"]["email"] == "user_individual@no_team.com"
    assert data["user"]["first_name"] == "user_individual"
    assert data["user"]["last_name"] == "test"
    assert data["teams"] == []


@pytest.mark.asyncio
async def test_whoami_with_team(async_client: AsyncClient, db):
    response = await async_client.get(
        "/api/v1/whoami", headers={"test_username": "user_1_team_1@team1.com"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["user"]["email"] == "user_1_team_1@team1.com"
    assert data["user"]["first_name"] == "user_1_team_1"
    assert data["user"]["last_name"] == "test"
    (org,) = data["teams"]
    assert org["name"] == "Team 1"


@pytest.mark.asyncio
async def test_api_key_lifecycle(async_client: AsyncClient, db):
    key_name = str(uuid.uuid4())
    response = await async_client.post(
        "/api/v1/auth/api_key",
        data={"name": key_name},
        content_type="application/json",
        headers={"test_username": "user_individual@no_team.com"},
    )
    assert response.status_code == 200, response.json()
    api_key = response.json()
    assert len(api_key) > 64  # secure
    response = await async_client.get(
        "/api/v1/api_keys", headers={"test_username": "user_individual@no_team.com"}
    )
    assert response.status_code == 200
    (data_filtered,) = [item for item in response.json() if item["key_name"] == key_name]
    assert data_filtered["key_name"] == key_name
    assert data_filtered["is_active"]
    assert api_key.startswith(data_filtered["key_start"])
    data_id = data_filtered["id"]
    response = await async_client.delete(
        f"/api/v1/api_key/{data_id}", headers={"test_username": "user_individual@no_team.com"}
    )
    assert response.status_code == 200

    response = await async_client.get(
        "/api/v1/api_keys", headers={"test_username": "user_individual@no_team.com"}
    )
    assert response.status_code == 200
    data_filtered = [item for item in response.json() if item["key_name"] == key_name]
    assert len(data_filtered) == 0
