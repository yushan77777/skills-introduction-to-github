"""The unified landing page: Monitoring or ETL, and nothing else.

Deliberately not a dashboard. It shows two doors and gets out of the way; each
application keeps its own home page behind its door.
"""

from django.shortcuts import render

SECTIONS = [
    {
        "name": "Monitoring",
        "tagline": "Server health",
        "description": "Per-directory disk usage with drill-down, history "
                       "trends and a treemap view.",
        "url_name": "home:index",
        "icon": "monitor",
    },
    {
        "name": "ETL",
        "tagline": "Data pipelines",
        "description": "Run, watch and stop the Oracle, Greenplum, CSV and "
                       "Parquet pipelines on this server.",
        "url_name": "etl:index",
        "icon": "etl",
    },
]


def index(request):
    return render(request, "portal/index.html", {"sections": SECTIONS})
