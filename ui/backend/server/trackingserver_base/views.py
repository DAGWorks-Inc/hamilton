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
