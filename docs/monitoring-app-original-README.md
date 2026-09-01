# Monitoring Web App

A Django website for internal system monitoring. The home page is an app
launcher — each monitoring tool is its own Django app, so new pages can be
added over time without touching the existing ones.

**Apps**

| App | URL | What it does |
|-----|-----|--------------|
| Home | `/` | Landing page listing all monitoring apps |
| Disk Usage | `/disk-usage/` | Per-directory server disk usage from Greenplum |

## Disk Usage features

- Defaults to the directories at the **minimum `depth`** in `server_disk_usage`,
  ordered by size (largest first) with proportional bars and share-of-level %.
- Shows the **source table name, total size and scan timestamps** in the header.
- **Double-click** a directory (or press Enter) to drill down one depth level;
  the breadcrumb navigates back up.
- **Single click** a directory to load its **history trend** from
  `server_disk_usage_hist_cdc` — the chart sits at the bottom of the page and is
  always visible (no scrolling). The current snapshot value is appended so the
  trend always ends at the latest scan, even though the CDC table only stores
  changed sizes.
- **Treemap** button opens a color-coded treemap of the selected folder's
  contents (box area = size, darker blue = larger). Hover for details,
  double-click a box to drill down.
- Light + dark theme with a toggle in the top bar. No external JS/CSS
  dependencies — everything is self-contained for offline internal servers.

## Setup

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/python manage.py migrate        # Django internals (local sqlite)
```

### Configuration

All Greenplum settings live in **`config.ini`** (never in code):

```ini
[greenplum]
driver = postgresql        # Greenplum is PostgreSQL-compatible (psycopg2)
host = your-greenplum-host.internal
port = 5432
database = monitoring
username = gpadmin
password = ...
# or set `url = postgresql://user:pass@host:5432/db` instead

[tables]
disk_usage_table = server_disk_usage
disk_usage_hist_table = server_disk_usage_hist_cdc
```

Point the app at a different config file with the `MONITORING_CONFIG`
environment variable.

### Run

```bash
./venv/bin/python manage.py runserver 0.0.0.0:8000
```

### Try it without Greenplum (demo mode)

```bash
./venv/bin/python scripts/make_demo_data.py
MONITORING_CONFIG=config.demo.ini ./venv/bin/python manage.py runserver
```

This builds `demo_data/demo.sqlite` with the same two tables and ~90 days of
generated history.

## Adding a new monitoring app later

1. `./venv/bin/python manage.py startapp my_app`
2. Add `'my_app'` to `INSTALLED_APPS` in `monitoring_site/settings.py`.
3. Add `path("my-app/", include("my_app.urls"))` in `monitoring_site/urls.py`.
4. Add a card for it in `home/views.py` (`APPS` list) so it appears on the
   home page, and a nav link in `templates/base.html` if desired.

## Production notes

- Set `DEBUG = False` and a real `SECRET_KEY` / `ALLOWED_HOSTS` in
  `monitoring_site/settings.py` before exposing it.
- `./venv/bin/python manage.py collectstatic` gathers static files into
  `staticfiles/` for serving via nginx/apache; or run behind gunicorn:
  `gunicorn monitoring_site.wsgi`.
- Keep `config.ini` readable only by the service user (`chmod 600 config.ini`).
