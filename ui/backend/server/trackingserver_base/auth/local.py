import logging
from typing import List, Optional, Tuple

from django.http import HttpRequest
from ninja.security.http import HttpAuthBase
from trackingserver_auth.models import Team, User, UserTeamMembership
from trackingserver_base.auth.sync import ensure_user_only_part_of_orgs

logger = logging.getLogger(__name__)


class LocalAPIAuthenticator(HttpAuthBase):
    async def __call__(self, request: HttpRequest) -> Optional[Tuple[User, List[Team]]]:
        """This is an authentication client for local mode. It will ensure a user exists with
        the appropriate username.
        """
        username = request.headers.get("x-api-user")
        return await self.ensure(username)

    @staticmethod
    async def ensure(username: str):
        try:
            user = await User.objects.aget(email=username)
        except User.DoesNotExist:
            logger.warning(f"Creating new user {username} for local mode")
            user = User(
                first_name=username.split("@")[0],
                last_name="local account",  # TODO -- get the actual last name
                email=username,
            )
            await user.asave()

            public = await Team.objects.aget(name="Public")
            await ensure_user_only_part_of_orgs(user, [public])
            return user, [public]
        teams = [
            item.team
            async for item in UserTeamMembership.objects.filter(user=user)
            .all()
            .prefetch_related("team")
        ]
        return user, teams
