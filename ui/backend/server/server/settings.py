import os
import socket
from pathlib import Path


def get_from_env(
    var_name: str, valid_values: list = None, allow_missing: bool = False, default_value: str = None
):
    value = os.environ.get(var_name)
    if value is None and not allow_missing:
        raise ValueError(f"Missing environment variable {var_name}")
    if valid_values is not None and value not in valid_values:
        raise ValueError(f"Invalid value for {var_name}: {value}")
    if value is None:
        return default_value
    return value


# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = get_from_env("DJANGO_SECRET_KEY")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get("DJANGO_DEBUG", "False") == "True"

HAMILTON_ENV = get_from_env("HAMILTON_ENV", ["integration_tests", "local", "dev", "prod"])

PROPEL_AUTH_API_KEY = get_from_env("PROPEL_AUTH_API_KEY", allow_missing=True)
PROPEL_AUTH_URL = get_from_env("PROPEL_AUTH_URL", allow_missing=True)

ALLOWED_HOSTS = ["localhost", "backend", "0.0.0.0", "127.0.0.1"]

HAMILTON_BLOB_STORE = get_from_env("HAMILTON_BLOB_STORE", ["local", "s3"])

HAMILTON_BLOB_STORE_PARAMS = (
    {
        "bucket_name": get_from_env("HAMILTON_S3_BUCKET", allow_missing=False),
        "region_name": get_from_env("HAMILTON_S3_REGION", allow_missing=False),
        "global_prefix": get_from_env("HAMILTON_ENV", allow_missing=False),
    }
    if HAMILTON_BLOB_STORE == "s3"
    else {
        "base_dir": get_from_env("HAMILTON_LOCAL_BLOB_DIR", allow_missing=False),
    }
)

hostname = socket.gethostname()
local_ip = socket.gethostbyname(hostname)

ALLOWED_HOSTS = ALLOWED_HOSTS + [local_ip] + [hostname]

# Application definition

INSTALLED_APPS = [
    "trackingserver_base.apps.TrackingServerConfig",
    "trackingserver_auth.apps.TrackingServerAuthConfig",
    "trackingserver_projects.apps.TrackingServerProjectConfig",
    "trackingserver_template.apps.TrackingServerTemplateConfig",
    "trackingserver_run_tracking.apps.TrackingServerRunTracking",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_extensions",
]

MIDDLEWARE = [
    "server.middleware.healthcheck.HealthCheckMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "server.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "server.wsgi.application"

STATIC_ROOT = "./staticfiles"

# Database
# https://docs.djangoproject.com/en/4.1/ref/settings/#databases

db_password = get_from_env("DB_PASSWORD")
db_host = get_from_env("DB_HOST")
db_user = get_from_env("DB_USER")
db_name = get_from_env("DB_NAME")
db_port = get_from_env("DB_PORT")

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": db_name,
        "USER": db_user,
        "PASSWORD": db_password,
        "HOST": db_host,
        "PORT": db_port,
        "TEST": {
            "NAME": "test_dagworks",
        },
    },
}

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# Internationalization
# https://docs.djangoproject.com/en/4.1/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.1/howto/static-files/

STATIC_URL = "static/"

# Default primary key field type
# https://docs.djangoproject.com/en/4.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# REST STUFF
REST_FRAMEWORK = {
    # your other DRF settings here
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "special": {
            "()": "colorlog.ColoredFormatter",
            "format": "%(log_color)s[%(asctime)s] %(message)s",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "special",
            # "filters": ["truncate_sql"],
        },
    },
    "root": {
        "handlers": ["console"],
        "level": os.getenv("DJANGO_LOG_LEVEL", "INFO"),
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": os.getenv("DJANGO_LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # "django.db.backends": {
        #     "level": "DEBUG",
        #     "handlers": ["console"],
        #     "propagate": False,
        # },
    },
}
if HAMILTON_ENV not in ["integration_tests", "local", "dev"]:
    LOGGING["loggers"]["ddtrace"] = {
        "handlers": ["console"],
        "level": "WARNING",
    }

DATA_UPLOAD_MAX_MEMORY_SIZE = 2621440 * 100  # 250MB
SILKY_PYTHON_PROFILER = False

if HAMILTON_ENV == "dev":
    # LOGGING["loggers"]["django.db.backends"] = {
    #     "level": "DEBUG",
    #     "handlers": ["console"],
    #     "propagate": False,
    # }
    LOGGING["filters"] = {
        "truncate_sql": {
            "()": "trackingserver_base.middleware.custom_logging.TruncateSQLFilter",
        }
    }
    LOGGING["handlers"]["console"]["filters"] = ["truncate_sql"]
    SILKY_PYTHON_PROFILER = False
    # MIDDLEWARE.insert(0, "silk.middleware.SilkyMiddleware")
    MIDDLEWARE.append("trackingserver_base.middleware.timing_middleware.TimingMiddleware")
    # INSTALLED_APPS.append("silk")
