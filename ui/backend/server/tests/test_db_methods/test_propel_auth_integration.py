import dataclasses
import uuid
from typing import List, Optional, TypedDict

import pytest
from django.forms import model_to_dict
from propelauth_py import UnauthorizedException
from tests.test_db_methods.utils import (
    assert_user_only_part_of_teams,
    assert_user_state,
    setup_random_user_with_n_teams,
    setup_sample_user_random,
)
from trackingserver_auth.models import Team
from trackingserver_base.auth.propelauth import PropelAuthBearerTokenAuthenticator, synchronize_auth

# I find thi far preferable to standard mocking when
# we don't have side-effects/care to count whether its called


class PropelAuthOrg(TypedDict):
    org_id: str
    org_name: str


@dataclasses.dataclass
class PropelAuthUser:
    user_id: str
    email: str


@dataclasses.dataclass
class MockPropelAuthInstance:
    user_id: Optional[str]
    user_email: Optional[str]
    orgs: List[PropelAuthOrg]
    exists: bool = True

    def fetch_user_metadata_by_user_id(self, user_id=None, include_orgs: bool = None):
        """Args don't matter as this is a one-time-use mock.
        Don't use this if the user shouldn't exist.

        @param user_id: User ID in propelauth
        @param include_orgs: Whether to include orgs, always true
        @return: User metadata
        """
        return {
            "user_id": self.user_id,
            "email": self.user_email,
            "org_id_to_org_info": {org["org_id"]: org for org in self.orgs},
        }

    def fetch_user_metadata_by_email(self, user_id=None, include_orgs: bool = None):
        """Args don't matter as this is a one-time-use mock.
        Don't use this if the user shouldn't exist.

        @param user_id: User ID in propelauth
        @param include_orgs: Whether to include orgs, always true
        @return: User metadata
        """
        return self.fetch_user_metadata_by_user_id()

    def validate_access_token_and_get_user(self, token: str) -> PropelAuthUser:
        if not self.exists:
            raise UnauthorizedException("User does not exist")
        return PropelAuthUser(user_id=self.user_id, email=self.user_email)


@pytest.mark.asyncio
async def test_ensure_user_propel_auth_user_does_not_exist_yet_no_teams(db):
    auth_provider_user_id = str(uuid.uuid4())
    user_email_prefix = uuid.uuid4()
    email = f"{user_email_prefix}@test_ensure_user_propel_auth_user_does_not_exist_no_teams.io"
    auth_instance = MockPropelAuthInstance(
        user_id=auth_provider_user_id,
        user_email=email,
        orgs=[],
    )
    await assert_user_state(email=email, exists=False)
    user_returned, teams_returned = await synchronize_auth(
        email=email, propel_auth_instance=auth_instance
    )

    user = await assert_user_state(email=email, exists=True)
    assert user.email == email
    assert model_to_dict(user_returned) == model_to_dict(user)
    # Public
    assert len(teams_returned) == 1
    assert teams_returned[0].name == "Public"
    assert user.auth_provider_user_id == auth_provider_user_id


@pytest.mark.asyncio
async def test_ensure_user_exists_already_exists_no_team(db):
    user = await setup_sample_user_random()
    auth_instance = MockPropelAuthInstance(
        user_id=user.auth_provider_user_id,
        user_email=user.email,
        orgs=[],
    )
    user_returned, teams_returned = await synchronize_auth(
        email=user.email, propel_auth_instance=auth_instance
    )

    user_returned = await assert_user_state(email=user.email, exists=True)
    assert user.email == user_returned.email
    assert model_to_dict(user_returned) == model_to_dict(user)
    # Public
    assert len(teams_returned) == 1
    assert teams_returned[0].name == "Public"
    assert user.auth_provider_user_id == user_returned.auth_provider_user_id


@pytest.mark.asyncio
async def test_ensure_user_propel_auth_user_does_not_exist_yet_multiple_teams(db):
    user, teams = await setup_random_user_with_n_teams(3)
    auth_instance = MockPropelAuthInstance(
        user_id=user.auth_provider_user_id,
        user_email=user.email,
        orgs=[
            {"org_id": team.auth_provider_organization_id, "org_name": team.name} for team in teams
        ],
    )
    user_returned, teams_returned = await synchronize_auth(
        email=user.email, propel_auth_instance=auth_instance
    )

    user_returned = await assert_user_state(email=user.email, exists=True)
    assert user.email == user_returned.email
    assert model_to_dict(user_returned) == model_to_dict(user)
    assert len(teams_returned) == len(teams) + 1
    assert user.auth_provider_user_id == user.auth_provider_user_id
    teams_user_should_be_part_of = [
        item
        async for item in Team.objects.filter(
            auth_provider_organization_id__in=[team.auth_provider_organization_id for team in teams]
        )
    ]
    teams_user_should_be_part_of.append(await Team.objects.aget(name="Public"))
    await assert_user_only_part_of_teams(user, teams_user_should_be_part_of)


@pytest.mark.asyncio
async def test_ensure_user_propel_auth_user_already_exists_multiple_teams(db):
    auth_provider_user_id = str(uuid.uuid4())
    user_email_prefix = uuid.uuid4()
    email = (
        f"{user_email_prefix}@test_ensure_user_propel_auth_user_does_not_exist_multiple_teams.io"
    )
    teams = [
        {"org_id": str(uuid.uuid4()), "org_name": str(uuid.uuid4())},
        {"org_id": str(uuid.uuid4()), "org_name": str(uuid.uuid4())},
        {"org_id": str(uuid.uuid4()), "org_name": str(uuid.uuid4())},
    ]
    auth_instance = MockPropelAuthInstance(
        user_id=auth_provider_user_id,
        user_email=email,
        orgs=teams,
    )
    await assert_user_state(email=email, exists=False)
    user_returned, teams_returned = await synchronize_auth(
        email=email, propel_auth_instance=auth_instance
    )

    user = await assert_user_state(email=email, exists=True)
    assert user.email == email
    assert model_to_dict(user_returned) == model_to_dict(user)
    assert len(teams_returned) == len(teams) + 1
    teams_user_should_be_part_of = [
        item
        async for item in Team.objects.filter(
            auth_provider_organization_id__in=[team["org_id"] for team in teams]
        )
    ]
    teams_user_should_be_part_of.append(await Team.objects.aget(name="Public"))
    await assert_user_only_part_of_teams(user, teams_user_should_be_part_of)


@dataclasses.dataclass
class MockRequest:
    headers: dict[str, str]


@pytest.mark.asyncio
async def test_propel_auth_bearer_token_authenticator_valid(db):
    user, teams = await setup_random_user_with_n_teams(3)
    authenticator = PropelAuthBearerTokenAuthenticator(
        propel_auth_instance=MockPropelAuthInstance(
            user_id=user.auth_provider_user_id,
            user_email=user.email,
            orgs=[
                {"org_id": team.auth_provider_organization_id, "org_name": team.name}
                for team in teams
            ],
            exists=True,
        )
    )
    user, teams_authenticated = await authenticator(
        MockRequest(
            headers={"Authorization": "Bearer valid_bearer_token"},
        ),
    )

    # Don't need to assert so much as we already have it done above
    assert user.email == user.email
    assert len(teams_authenticated) == len(teams) + 1


@pytest.mark.asyncio
async def test_propel_auth_bearer_token_authenticator_invalid(db):
    authenticator = PropelAuthBearerTokenAuthenticator(
        propel_auth_instance=MockPropelAuthInstance(
            user_id=None,
            user_email=None,
            orgs=[],
            exists=False,
        )
    )
    assert (
        await authenticator(
            MockRequest(
                headers={"Authorization": "Bearer invalid_bearer_token"},
            ),
        )
        is None
    )
