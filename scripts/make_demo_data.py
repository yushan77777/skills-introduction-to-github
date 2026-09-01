"""Generate demo_data/demo.sqlite with realistic-looking disk usage data.

Creates the same two tables the Greenplum collector fills
(server_disk_usage + server_disk_usage_hist_cdc) so the web app can be
tried locally with driver = sqlite before pointing it at Greenplum.

Usage:  python scripts/make_demo_data.py
"""

import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUT = BASE_DIR / "demo_data" / "demo.sqlite"

random.seed(42)

# Directory tree: {path: base size of the dir's own files in KB}
# Parents get the sum of their subtree. Min depth is 2 (e.g. /data/warehouse).
TREE = {
    "/data/warehouse": 0,
    "/data/warehouse/sales": 0,
    "/data/warehouse/sales/2025": 48_000_000,
    "/data/warehouse/sales/2026": 26_000_000,
    "/data/warehouse/inventory": 31_000_000,
    "/data/warehouse/staging": 9_500_000,
    "/data/backups": 0,
    "/data/backups/daily": 62_000_000,
    "/data/backups/weekly": 38_000_000,
    "/data/backups/monthly": 21_000_000,
    "/opt/apps": 0,
    "/opt/apps/web_portal": 0,
    "/opt/apps/web_portal/static": 1_800_000,
    "/opt/apps/web_portal/media": 6_400_000,
    "/opt/apps/etl_jobs": 4_200_000,
    "/opt/apps/legacy_crm": 2_900_000,
    "/var/log_archive": 0,
    "/var/log_archive/app": 5_600_000,
    "/var/log_archive/db": 12_400_000,
    "/var/log_archive/system": 2_100_000,
    "/home/shared": 0,
    "/home/shared/reports": 3_400_000,
    "/home/shared/exports": 7_800_000,
}


def depth_of(path):
    return len([p for p in path.strip("/").split("/") if p])


def children_of(path):
    prefix = path + "/"
    d = depth_of(path) + 1
    return [p for p in TREE if p.startswith(prefix) and depth_of(p) == d]


def subtree_size(path, own_sizes):
    total = own_sizes[path]
    for child in children_of(path):
        total += subtree_size(child, own_sizes)
    return total


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.unlink(missing_ok=True)
    conn = sqlite3.connect(OUT)
    cur = conn.cursor()
    ddl = """
        CREATE TABLE {name} (
            size_kb text,
            directory text,
            depth bigint,
            scan_start_time timestamp,
            scan_end_time timestamp
        )
    """
    cur.execute(ddl.format(name="server_disk_usage"))
    cur.execute(ddl.format(name="server_disk_usage_hist_cdc"))

    leaves = [p for p in TREE if not children_of(p)]
    own = dict(TREE)

    # 45 scans, every ~2 days, ending today; leaf sizes random-walk and
    # only changed values land in the CDC history table.
    n_scans = 45
    start = datetime(2026, 7, 15, 2, 0, 0) - timedelta(days=2 * (n_scans - 1))
    previous = {}
    snapshot_rows = []

    for scan_idx in range(n_scans):
        scan_start = start + timedelta(
            days=2 * scan_idx, minutes=random.randint(0, 50)
        )
        scan_end = scan_start + timedelta(minutes=random.randint(4, 18))

        for leaf in leaves:
            drift = random.uniform(-0.02, 0.045)  # slow organic growth
            if random.random() < 0.07:            # occasional cleanup/spike
                drift += random.choice([-0.30, 0.35])
            own[leaf] = max(10_000, own[leaf] * (1 + drift))

        rows = []
        for path in TREE:
            size = int(subtree_size(path, own))
            rows.append(
                (
                    str(size),
                    path,
                    depth_of(path),
                    scan_start.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    scan_end.strftime("%Y-%m-%d %H:%M:%S.%f"),
                )
            )

        # CDC: insert only when the size changed vs the previous scan
        for row in rows:
            size, path = row[0], row[1]
            if previous.get(path) != size:
                cur.execute(
                    "INSERT INTO server_disk_usage_hist_cdc VALUES (?,?,?,?,?)", row
                )
                previous[path] = size

        if scan_idx == n_scans - 1:
            snapshot_rows = rows

    # snapshot table holds only the latest scan (collector truncates + reloads)
    cur.executemany(
        "INSERT INTO server_disk_usage VALUES (?,?,?,?,?)", snapshot_rows
    )

    conn.commit()
    snap = cur.execute("SELECT COUNT(*) FROM server_disk_usage").fetchone()[0]
    hist = cur.execute("SELECT COUNT(*) FROM server_disk_usage_hist_cdc").fetchone()[0]
    conn.close()
    print(f"Wrote {OUT}")
    print(f"  server_disk_usage:          {snap} rows")
    print(f"  server_disk_usage_hist_cdc: {hist} rows")


if __name__ == "__main__":
    main()
