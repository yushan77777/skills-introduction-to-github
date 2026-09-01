from django.urls import path

from . import views

app_name = "etl"

urlpatterns = [
    path("", views.index, name="index"),
    path("api/jobs/", views.api_jobs, name="api_jobs"),
    path("api/config/", views.api_config, name="api_config"),
    path("api/validate/", views.api_validate, name="api_validate"),
    path("api/run/", views.api_run, name="api_run"),
    path("api/status/", views.api_status, name="api_status"),
    path("api/stop/", views.api_stop, name="api_stop"),
    path("api/history/", views.api_history, name="api_history"),
    path("api/log/", views.api_log, name="api_log"),
]
