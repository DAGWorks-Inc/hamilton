import functools
import inspect
import typing

from ninja.errors import HttpError


class EndpointCallable(typing.Protocol):
    # pylint: disable=E704
    async def __call__(self, request: typing.Any, **kwargs) -> typing.Any: ...


class permission:
    """Decorator that pre-checks any resources an endpoint modifies.
    This goes hand-in-hand with the functions below..."""

    DECORATED_ATTR = "decorated_with_auth_function"

    def __init__(
        self, auth_function: typing.Callable[..., typing.Awaitable[typing.Tuple[bool, str]]]
    ):
        """Initializes an auth_with decorator

        @param auth_function:
        @param resource_name:
        @param operation:
        """
        self.auth_function = auth_function

    def __call__(self, endpoint: EndpointCallable) -> EndpointCallable:
        setattr(endpoint, self.DECORATED_ATTR, self)
        endpoint_signature = inspect.signature(endpoint)
        auth_function_signature = inspect.signature(self.auth_function)
        if not set(endpoint_signature.parameters.keys()).issuperset(
            set(auth_function_signature.parameters.keys())
        ):
            raise ValueError(
                f"Auth function {self.auth_function} must have all of the "
                f"parameters of the endpoint {endpoint}"
            )

        @functools.wraps(endpoint)
        async def output_function(request, *args, **kwargs):
            try:
                can_do, message = await self.auth_function(
                    request,
                    **{
                        key: value
                        for key, value in kwargs.items()
                        if key in auth_function_signature.parameters
                    },
                )
            except Exception as e:
                raise ValueError(self.auth_function.__name__) from e
            if not can_do:
                raise HttpError(404, message)
            return await endpoint(request, **kwargs)

        return output_function


async def not_allowed(request: typing.Any) -> typing.Tuple[bool, str]:
    """Checks if a user can create an API key.

    @param request:
    @return:
    """
    return False, "This operation is not allowed."


async def allowed(request: typing.Any) -> typing.Tuple[bool, str]:
    """Checks if a user can create an API key.

    @param request:
    @return:
    """
    return True, ""
