from django.shortcuts import render

# Registry of monitoring apps shown on the home page.
# To add a new app later: build the Django app, then append a card here.
APPS = [
    {
        "name": "Disk Usage",
        "description": "Per-directory server disk usage with drill-down, "
        "history trends and a treemap view.",
        "url_name": "disk_usage:index",
        "icon": "disk",
        "available": True,
    },
    {
        "name": "More apps coming soon",
        "description": "New monitoring apps will appear here as they are added.",
        "url_name": None,
        "icon": "plus",
        "available": False,
    },
]


def index(request):
    return render(request, "home/index.html", {"apps": APPS})
