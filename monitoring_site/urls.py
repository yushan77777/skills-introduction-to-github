"""URL configuration for the unified ETL & Monitoring platform.

Layout:

    /                 unified landing page — Monitoring or ETL
    /monitoring/      the Monitoring app launcher (was ``/`` before the merge)
    /disk-usage/      Monitoring: disk usage — unchanged, existing links still work
    /etl/             the ETL console
    /admin/           Django admin — unchanged

Only the landing page moved. Every Monitoring URL that existed before still
resolves to the same view, and ``home:index`` still names the Monitoring
launcher, so its templates needed no rewriting.
"""
from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    # Unified entry point.
    path("", include("portal.urls")),
    # Monitoring — existing apps, unchanged.
    path("monitoring/", include("home.urls")),
    path("disk-usage/", include("disk_usage.urls")),
    # ETL — new app, kept entirely separate from the monitoring code.
    path("etl/", include("etl.urls")),
]
