import logging
import os

import aiohttp
from trackingserver_auth.models import User

logger = logging.getLogger(__name__)
"""When you have signups to the service, you can notify the team. This does it via slack.
Note if the proper env vars are not set, this will bypass doing so"""

# TODO -- ensure that in `PROD` mode, these error out if not set
# Probably in django config
SLACK_WEBHOOK_URL = os.environ.get("HAMILTON_SLACK_WEBHOOK_URL")
APP_URL = os.environ.get("HAMILTON_APP_URL")
environment = os.environ.get("HAMILTON_ENV", "dev")


async def user_signup_notification(user: User):
    """Sends a notification to Slack that a user has signed up.
    @param user: The user that signed up
    """
    if SLACK_WEBHOOK_URL is None:
        logger.warning("No slack webhook URL set, not sending notification.")
        return
    await send_slack_notification(
        {
            "text": f"User {user.email} with name: {user.first_name} {user.last_name} has signed up at "
            f"{APP_URL}, env: `{environment}`!"
        }
    )


async def send_slack_notification(contents: dict):
    """Sends a notification to Slack.

    @param contents: dict of slack notification contents
    @return: None
    """
    if SLACK_WEBHOOK_URL is None:
        logger.warning("No slack webhook URL set, not sending notification.")
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(SLACK_WEBHOOK_URL, json=contents) as response:
                logger.info(f"Sent slack notification: {contents}")
                response.raise_for_status()
    except Exception as e:
        logger.exception(f"Failed to send slack notification: {e}")
