# this file is not needed and superseded by api.py
# # Create your views here.
# from django.http import HttpResponse, JsonResponse
# from django.shortcuts import render
#
# # from ninja import NinjaAPI
# from ninja import Router
#
# router = Router()
#
#
# @router.get("")
# def index(request):
#     return HttpResponse("Hello, world. You're at the gitserver index.")
#
# @router.get("code")
# def code(request):
#     data = {"repo": "foobar", "files": "Finland", "is_active": True, "count": 28}
#     # return JsonResponse(data)
#     return data
import os

from django.conf import settings
from django.http import HttpResponse


def serve_frontend(request):
    """
    Serves the index.html file or other static files directly from the build folder.
    """
    path = request.path.strip("/")
    if not path:
        path = "index.html"
    file_path = os.path.join(settings.BASE_DIR, "build", path)
    if os.path.exists(file_path):
        with open(file_path, "rb") as file:
            return HttpResponse(file.read(), content_type="text/html")
    return HttpResponse("File not found", status=404)
