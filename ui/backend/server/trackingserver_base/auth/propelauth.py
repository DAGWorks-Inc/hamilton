import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import cachetools.func
from asgiref.sync import sync_to_async
from cachetools import cached
from cachetools.keys import hashkey
from django.conf import settings
from django.http import HttpRequest
from ninja.compatibility import get_headers
from ninja.security import APIKeyQuery, HttpBearer
from propelauth_py import UnauthorizedException, init_base_auth
from trackingserver_auth.models import Team, User
from trackingserver_base.auth.api_keys import validate_api_key
from trackingserver_base.auth.sync import (
    ensure_team_exists,
    ensure_user_exists,
    ensure_user_only_part_of_orgs,
)

auth = None

logger = logging.getLogger(__name__)


def init():
    propel_auth_api_key = settings.PROPEL_AUTH_API_KEY
    propel_auth_url = settings.PROPEL_AUTH_URL

    if propel_auth_url is None or propel_auth_api_key is None:
        raise ValueError("PROPEL_AUTH_URL and PROPEL_AUTH_API_KEY must be set.")

    global auth
    auth = init_base_auth(propel_auth_url, propel_auth_api_key)


def fetch_user_metadata_by_email(
        email: str, include_orgs: bool, propel_auth_instance
) -> Dict[str, Any]:
    """Fetches the metadata for a user by email.

    @param email: Email to fetch
    @param propel_auth_instance: Propel auth instance to use
    @return: User metadata
    """
    return propel_auth_instance.fetch_user_metadata_by_email(email, include_orgs=include_orgs)


if settings.HAMILTON_ENV != "integration_tests":
    cache = cachetools.TTLCache(maxsize=1000, ttl=180)
    fetch_user_metadata_by_email = cached(
        cache, key=lambda email, include_orgs, propel_auth_instance: hashkey(email, include_orgs)
    )(fetch_user_metadata_by_email)


async def synchronize_auth(email: str, propel_auth_instance=auth) -> Tuple[User, List[Team]]:
    """Validates that the user exists in the database, and that the user is part of the
    associated organizations.

    If the user is not part of the associated organizations, this creates them and
    adds to the linking.

    Note that this will throw an exception if it doesn't exist -- validating the JWTs using the propel
    auth API is expected to occur before this.

    @param user: User from propelauth
    @return: The user, and the list of organizations
    """
    logger.info(f"Ensuring user {email} exists")
    # user_with_org_data = fetch_user_metadata_by_email(email, include_orgs=True)
    user_with_org_data = await sync_to_async(
        # propel_auth_instance.fetch_user_metadata_by_email,
        fetch_user_metadata_by_email,
        thread_sensitive=True,
    )(email, include_orgs=True, propel_auth_instance=propel_auth_instance)
    logger.info(f"User {email} exists")
    user_ensured = await ensure_user_exists(
        user_with_org_data["user_id"], user_with_org_data["email"]
    )
    orgs = []
    for org in user_with_org_data["org_id_to_org_info"].values():
        org_ensured = await ensure_team_exists(org["org_id"], org["org_name"])
        orgs.append(org_ensured)
    # not index but this will be plenty fast
    public = await Team.objects.aget(name="Public")
    if public not in orgs:
        orgs.append(public)
    await ensure_user_only_part_of_orgs(user_ensured, orgs)
    # TODO -- remove users from orgs if its been updated...
    return user_ensured, orgs


class PropelAuthBearerTokenAuthenticator(HttpBearer):
    """Basic auth provider. Note that this is overloaded -- it actually synchronizes
    the DB with the auth provider. See `ensure_user` for more details."""

    def __init__(self, propel_auth_instance=auth):
        super(PropelAuthBearerTokenAuthenticator).__init__()
        self.propel_auth_instance = propel_auth_instance

    async def __call__(self, request: HttpRequest) -> Optional[Any]:
        """This clones the django ninja authenticator, as the coroutine won't be awaited otherwise.
        TODO -- remove this when this is fixed: https://github.com/vitalik/django-ninja/issues/44.

        @param request:
        @return:
        """
        headers = get_headers(request)
        auth_value = headers.get(self.header)
        if not auth_value:
            return None
        parts = auth_value.split(" ")

        if parts[0].lower() != self.openapi_scheme:
            if settings.DEBUG:
                logger.error(f"Unexpected auth - '{auth_value}'")
            return None
        token = " ".join(parts[1:])
        return await self.authenticate(request, token)

    async def authenticate(self, request, token) -> Optional[Tuple[User, List[Team]]]:
        try:
            # TODO -- explore if we can use this later during auth syncrhonization...
            # We should have the information we need to handle this
            user = self.propel_auth_instance.validate_access_token_and_get_user("Bearer " + token)
            user, orgs = await synchronize_auth(
                user.email, propel_auth_instance=self.propel_auth_instance
            )
            return user, orgs
        except UnauthorizedException as e:
            logger.exception(e)
            return None


class PropelAuthAPIKeyAuthenticator(APIKeyQuery):
    param_name = "x-api-key"

    async def __call__(self, request: HttpRequest) -> Optional[Any]:
        api_key = request.headers.get("x-api-key")
        return await self.authenticate(request, api_key)

    async def authenticate(
            self, request, key, propel_auth_instance=auth
    ) -> Optional[Tuple[User, List[Team]]]:
        # TODO _- handle this using the builtin key with param_name
        user_email = request.headers.get("x-api-user")
        is_valid = await validate_api_key(user_email, api_key=key)
        if not is_valid:
            return None
        # We ensure any synchronization is done here
        return await synchronize_auth(user_email)
