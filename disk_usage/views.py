"""Views + JSON APIs for the disk usage app."""

import functools

from django.http import JsonResponse
from django.shortcuts import render

from . import gp


def index(request):
    return render(
        request,
        "disk_usage/index.html",
        {
            "snapshot_table": gp.table_name("disk_usage_table"),
            "hist_table": gp.table_name("disk_usage_hist_table"),
        },
    )


def _serialize(row):
    """Make DB row values JSON-friendly (Decimal -> float, datetime -> str)."""
    out = {}
    for key, value in row.items():
        if key == "size_kb":
            out[key] = float(value) if value is not None else 0.0
        elif key in ("scan_start_time", "scan_end_time"):
            out[key] = str(value) if value is not None else None
        else:
            out[key] = value
    return out


def json_api(view):
    """Wrap an API view so DB/config errors come back as JSON, not HTML 500s."""

    @functools.wraps(view)
    def wrapper(request, *args, **kwargs):
        try:
            return view(request, *args, **kwargs)
        except Exception as exc:  # surfaced to the UI as a friendly banner
            return JsonResponse({"error": str(exc)}, status=500)

    return wrapper


_ROW_COLUMNS = """
    directory,
    depth,
    CAST(size_kb AS NUMERIC) AS size_kb,
    scan_start_time,
    scan_end_time
"""


@json_api
def api_directories(request):
    """List directories for one level.

    Without ?parent= -> all rows at the minimum depth in the snapshot table.
    With ?parent=/some/path -> its direct children (depth = parent depth + 1).
    Always ordered by size descending.
    """
    table = gp.table_name("disk_usage_table")
    parent = (request.GET.get("parent") or "").strip()
    if parent and parent != "/":
        parent = parent.rstrip("/")

    parent_row = None
    if parent:
        found = gp.query(
            f"SELECT {_ROW_COLUMNS} FROM {table} WHERE rtrim(directory, '/') = %s",
            (parent,),
        )
        if not found:
            return JsonResponse(
                {"error": f"Directory not found in {table}: {parent}"}, status=404
            )
        parent_row = _serialize(found[0])
        pattern = gp.escape_like(parent) + "/%"
        rows = gp.query(
            f"""
            SELECT {_ROW_COLUMNS}
            FROM {table}
            WHERE depth = %s
              AND rtrim(directory, '/') LIKE %s ESCAPE '\\'
            ORDER BY CAST(size_kb AS NUMERIC) DESC
            """,
            (parent_row["depth"] + 1, pattern),
        )
    else:
        rows = gp.query(
            f"""
            SELECT {_ROW_COLUMNS}
            FROM {table}
            WHERE depth = (SELECT MIN(depth) FROM {table})
            ORDER BY CAST(size_kb AS NUMERIC) DESC
            """
        )

    rows = [_serialize(r) for r in rows]
    scan_start = max(
        (r["scan_start_time"] for r in rows if r["scan_start_time"]), default=None
    )
    scan_end = max(
        (r["scan_end_time"] for r in rows if r["scan_end_time"]), default=None
    )
    if parent_row:
        scan_start = scan_start or parent_row["scan_start_time"]
        scan_end = scan_end or parent_row["scan_end_time"]

    return JsonResponse(
        {
            "table": table,
            "parent": parent_row,
            "rows": rows,
            "total_kb": sum(r["size_kb"] for r in rows),
            "scan_start_time": scan_start,
            "scan_end_time": scan_end,
        }
    )


@json_api
def api_history(request):
    """Disk-usage history for one directory.

    Reads the CDC history table (only changed sizes are recorded there) and
    appends the current snapshot value so the trend always ends at "now".
    """
    directory = (request.GET.get("directory") or "").strip()
    if not directory:
        return JsonResponse({"error": "Missing ?directory= parameter"}, status=400)
    if directory != "/":
        directory = directory.rstrip("/")

    hist_table = gp.table_name("disk_usage_hist_table")
    snap_table = gp.table_name("disk_usage_table")

    rows = gp.query(
        f"""
        SELECT scan_start_time, CAST(size_kb AS NUMERIC) AS size_kb
        FROM {hist_table}
        WHERE rtrim(directory, '/') = %s
        UNION ALL
        SELECT scan_start_time, CAST(size_kb AS NUMERIC) AS size_kb
        FROM {snap_table}
        WHERE rtrim(directory, '/') = %s
        ORDER BY scan_start_time
        """,
        (directory, directory),
    )

    # The current snapshot scan may also exist in the history table
    # (when the size changed on the latest run) — dedupe on timestamp.
    seen = set()
    points = []
    for row in rows:
        stamp = str(row["scan_start_time"])
        if stamp in seen:
            continue
        seen.add(stamp)
        points.append({"scan_start_time": stamp, "size_kb": float(row["size_kb"] or 0)})

    return JsonResponse({"table": hist_table, "directory": directory, "points": points})
