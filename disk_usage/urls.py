from django.urls import path

from . import views

app_name = "disk_usage"

urlpatterns = [
    path("", views.index, name="index"),
    path("api/directories/", views.api_directories, name="api_directories"),
    path("api/history/", views.api_history, name="api_history"),
]
