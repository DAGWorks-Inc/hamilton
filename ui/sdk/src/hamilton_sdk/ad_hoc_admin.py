import asyncio
import os
from typing import Callable

from hamilton_sdk.api import clients, constants


def create_or_get_project(
    project_name: str,
    username: str,
    client_factory: Callable[
        [str, str, str], clients.HamiltonClient
    ] = clients.BasicSynchronousHamiltonClient,
    api_key: str = None,
    hamilton_api_url=os.environ.get("HAMILTON_API_URL", constants.HAMILTON_API_URL),
) -> int:
    """Creates a project if it does not exist, by name. If multiple of the same name exist, this
    will error out. Will return the project ID for use later. Note names must (currently) be globally
    unique but that is not enforced by the API. We will likely be enforcing it and migrating later.

    :param project_name:
    :param username:
    :param client_factory:
    :param api_key:
    :param hamilton_api_url:
    :return:
    """
    client = client_factory(api_key, username, hamilton_api_url)
    out = client.create_or_get_project(project_name)
    return out


async def acreate_or_get_project(
    project_name: str,
    username: str,
    client_factory: Callable[
        [str, str, str], clients.HamiltonClient
    ] = clients.BasicAsynchronousHamiltonClient,
    api_key: str = None,
    hamilton_api_url=os.environ.get("HAMILTON_API_URL", constants.HAMILTON_API_URL),
) -> int:
    """Creates a project if it does not exist, by name. If multiple of the same name exist, this
    will error out. Will return the project ID for use later. Note names must (currently) be globally
    unique but that is not enforced by the API. We will likely be enforcing it and migrating later.

    :param project_name:
    :param username:
    :param client_factory:
    :param api_key:
    :param hamilton_api_url:
    :return:
    """
    client = client_factory(api_key, username, hamilton_api_url)
    out = await client.create_or_get_project(project_name)
    return out


if __name__ == "__main__":
    print(
        asyncio.run(
            acreate_or_get_project(
                project_name="test_does_not_exist_3",
                username="elijah@dagworks.io",
            )
        )
    )
