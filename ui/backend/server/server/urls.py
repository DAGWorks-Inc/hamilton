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

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, re_path
from django.views.generic import TemplateView
from django.views.static import serve

from . import api, default_views

# TODO -- ensure we didn't break the actual deployment
if settings.HAMILTON_ENV == "mini":
    # mini-mode
    # TODO -- do meda assets correctly -- this just hardcodes logo.png for now
    urlpatterns = [
        path("api/", api.api.urls),
        path("admin/", admin.site.urls),
        re_path(r"^logo\.png$", serve, {"document_root": settings.MEDIA_ROOT, "path": "logo.png"}),
        re_path(".*", TemplateView.as_view(template_name="index.html")),
    ] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
else:
    urlpatterns = [
        path("api/", api.api.urls),
        path("", default_views.root_index),
        path("admin/", admin.site.urls),
    ]

# HAMILTON_ENV = os.environ.get("HAMILTON_ENV", "dev")
# if HAMILTON_ENV == "dev":
#     urlpatterns += [path("silk/", include("silk.urls", namespace="silk"))]
