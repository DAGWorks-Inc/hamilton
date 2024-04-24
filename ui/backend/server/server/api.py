from django.conf import settings
from ninja import NinjaAPI
from trackingserver_auth import api as auth_api
from trackingserver_base import api as base_api
from trackingserver_base.auth import propelauth
from trackingserver_base.auth.local import LocalAPIAuthenticator
from trackingserver_base.auth.testing import TestAPIAuthenticator
from trackingserver_projects import api as project_api
from trackingserver_run_tracking import api as run_tracking_api
from trackingserver_template import api as template_api

env = settings.HAMILTON_ENV

if env == "local":
    api = NinjaAPI(auth=[LocalAPIAuthenticator()])
elif env == "integration_tests":
    api = NinjaAPI(auth=[TestAPIAuthenticator()])
else:
    propelauth.init()
    api = NinjaAPI(
        auth=[
            propelauth.PropelAuthBearerTokenAuthenticator(),
            propelauth.PropelAuthAPIKeyAuthenticator(),
        ]
    )
    # only use ddtrace in prod/staging
    from ddtrace import patch_all  # noqa: E402

    patch_all()


api.add_router("/", base_api.router)
api.add_router("/", auth_api.router)
api.add_router("/", project_api.router)
api.add_router("/", template_api.router)
api.add_router("/", run_tracking_api.router)
