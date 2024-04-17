"""server URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
import os

from django.contrib import admin
from django.urls import include, path  # include,

from . import api, default_views

# from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView


urlpatterns = [
    # path('api/schema/swagger-ui', SpectacularSwaggerView.as_view(), name='schema'),
    # path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path("api/", api.api.urls),
    path("", default_views.root_index),
    path("admin/", admin.site.urls),
]

HAMILTON_ENV = os.environ.get("HAMILTON_ENV", "dev")
if HAMILTON_ENV == "dev":
    urlpatterns += [path("silk/", include("silk.urls", namespace="silk"))]
