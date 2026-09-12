"""
File discovery, batching and processed-file tracking.

Three responsibilities, all streaming - the driver never holds the whole input
directory in memory:

``discover_files``
    Walks ``INPUT_PATH`` lazily with :func:`os.scandir` and yields one
    :class:`DiscoveredFile` at a time.

``ProcessedFileRegistry``
    The append-only ``processed/processed_files.csv``. Existing rows are never
    rewritten; a completed file is recorded by appending one line, flushed and
    ``fsync``-ed under an exclusive lock so a crash cannot leave a half written
    record. Lookups use an in-memory set of file keys built once per run.

``PendingBatchStore``
    The intent marker written *before* the Greenplum load and removed *after*
    the tracking CSV has been appended to. It is what makes the ETL restartable:
    a marker left behind by a crash tells the next run which batch was in flight,
    so it can ask Greenplum whether that batch actually committed and either
    promote the files to SUCCESS or leave them unprocessed.
"""

from __future__ import annotations

import csv
import errno
import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from fnmatch import fnmatch
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set

try:                                                   # POSIX only, optional
    import fcntl
except ImportError:                                    # pragma: no cover - Windows
    fcntl = None                                       # type: ignore

logger = logging.getLogger("atm_ejournal.files")

#: Header of processed_files.csv. Appended to, never rewritten.
PROCESSED_CSV_COLUMNS = [
    "file_name",
    "file_path",
    "file_key",
    "atm_no",
    "file_size",
    "file_mtime",
    "processed_date",
    "batch_id",
    "run_id",
    "records",
    "status",
]

PENDING_STATUS = "PENDING"
SUCCESS_STATUS = "SUCCESS"


@dataclass
class DiscoveredFile:
    """One input journal file and the identity used to track it."""

    path: str
    relative_path: str
    atm_no: str
    size: int
    mtime: float

    def key(self, key_mode: str = "path") -> str:
        """
        Identity written to (and matched against) the tracking CSV.

        ``path``        - relative path only; a file is processed once, ever.
        ``path_size``   - re-delivered file with a different size is processed again.
        ``path_mtime``  - also reacts to a changed modification time.
        """
        if key_mode == "path_size":
            return f"{self.relative_path}|{self.size}"
        if key_mode == "path_mtime":
            return f"{self.relative_path}|{self.size}|{int(self.mtime)}"
        return self.relative_path

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #


def _matches(name: str, patterns: Iterable[str]) -> bool:
    patterns = list(patterns)
    if not patterns:
        return True
    return any(fnmatch(name, pattern) or fnmatch(name.lower(), pattern.lower())
               for pattern in patterns)


def derive_atm_no(relative_path: str, input_path: str, folder_depth: int = 1) -> str:
    """
    ATM number carried by the directory layout (``ATM_EJOURNALS/<ATM_NO>/file``),
    matching the convention the existing parser uses. Falls back to the input
    directory name for a flat drop folder.
    """
    parts = relative_path.replace("\\", "/").split("/")
    if len(parts) > folder_depth >= 1:
        return parts[folder_depth - 1]
    return os.path.basename(os.path.normpath(input_path))


def discover_files(input_path: str,
                   patterns: Optional[Iterable[str]] = None,
                   folder_depth: int = 1,
                   min_file_age_seconds: int = 0,
                   sniff_unknown_extensions: bool = False,
                   skip_hidden: bool = True) -> Iterator[DiscoveredFile]:
    """
    Yield every journal file under ``input_path``, lazily and in a stable order.

    Only one directory listing is held at a time, so a directory holding
    millions of files is walked without ever building a list of them all.
    ``sniff_unknown_extensions`` reuses the parser's content sniffing for files
    whose name does not match ``FILE_PATTERN``; it opens files, so it is off by
    default for large inputs.
    """
    if not os.path.isdir(input_path):
        raise NotADirectoryError(f"input directory does not exist: {input_path}")

    patterns = list(patterns or [])
    now = datetime.now().timestamp()
    sniffer = None
    if sniff_unknown_extensions:
        from atm_ejournal_parser import _is_journal_file        # reuse existing logic
        sniffer = _is_journal_file

    for root, dirs, files in os.walk(input_path, followlinks=True):
        dirs[:] = sorted(d for d in dirs if not (skip_hidden and d.startswith(".")))
        for name in sorted(files):
            if skip_hidden and (name.startswith(".") or name.startswith("~$")):
                continue
            path = os.path.join(root, name)
            if not _matches(name, patterns):
                if sniffer is None or not sniffer(path):
                    continue
            try:
                stat = os.stat(path)
            except OSError as exc:
                logger.warning("skipping unreadable file %s: %s", path, exc)
                continue
            if not os.path.isfile(path):
                continue
            if min_file_age_seconds and (now - stat.st_mtime) < min_file_age_seconds:
                logger.debug("skipping %s, modified %.0fs ago (still being written?)",
                             name, now - stat.st_mtime)
                continue
            relative = os.path.relpath(path, input_path)
            yield DiscoveredFile(
                path=path,
                relative_path=relative,
                atm_no=derive_atm_no(relative, input_path, folder_depth),
                size=stat.st_size,
                mtime=stat.st_mtime,
            )


def iter_batches(files: Iterable[DiscoveredFile], batch_size: int) -> Iterator[List[DiscoveredFile]]:
    """
    Group a lazy stream of files into lists of ``batch_size``.

    Memory is bounded by one batch, whatever the size of the input directory.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    batch: List[DiscoveredFile] = []
    for item in files:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def format_batch_id(sequence: int, prefix: str = "BATCH") -> str:
    return f"{prefix}_{sequence:04d}"


# --------------------------------------------------------------------------- #
# Processed-file tracking
# --------------------------------------------------------------------------- #


class ProcessedFileRegistry:
    """
    Append-only record of the files that were successfully loaded to Greenplum.

    The CSV is never rewritten: :meth:`append_batch` opens it in append mode,
    writes one row per file, flushes and ``fsync``s under an exclusive lock, so a
    concurrent run or a crash cannot corrupt earlier rows.
    """

    def __init__(self, csv_path: str, key_mode: str = "path"):
        self.csv_path = csv_path
        self.key_mode = key_mode
        self._keys: Optional[Set[str]] = None

    # -- reading ----------------------------------------------------------- #

    def ensure_file(self) -> None:
        """Create the CSV with its header when it does not exist yet."""
        directory = os.path.dirname(os.path.abspath(self.csv_path))
        os.makedirs(directory, exist_ok=True)
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="", encoding="utf-8") as handle:
                csv.writer(handle).writerow(PROCESSED_CSV_COLUMNS)
            logger.info("created processed-file tracking CSV: %s", self.csv_path)

    def load_keys(self, refresh: bool = False) -> Set[str]:
        """
        Read the tracking CSV once and return the set of processed file keys.

        Only the key column is kept, so the footprint stays small even for
        millions of rows. Malformed lines are reported and skipped rather than
        aborting the run - a truncated last line from an earlier crash must not
        stop the ETL.
        """
        if self._keys is not None and not refresh:
            return self._keys

        keys: Set[str] = set()
        if not os.path.exists(self.csv_path):
            logger.info("no processed-file tracking CSV yet at %s - treating this as the "
                        "first execution", self.csv_path)
            self._keys = keys
            return keys

        malformed = 0
        duplicates = 0
        try:
            with open(self.csv_path, "r", newline="", encoding="utf-8", errors="replace") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None:
                    logger.warning("processed-file tracking CSV %s is empty", self.csv_path)
                    self._keys = keys
                    return keys
                if "file_key" not in reader.fieldnames:
                    logger.warning("processed-file tracking CSV %s has no 'file_key' column - "
                                   "falling back to 'file_path'", self.csv_path)
                for row in reader:
                    try:
                        if str(row.get("status", SUCCESS_STATUS)).upper() != SUCCESS_STATUS:
                            continue
                        key = (row.get("file_key") or row.get("file_path") or "").strip()
                        if not key:
                            malformed += 1
                            continue
                        if key in keys:
                            duplicates += 1
                            continue
                        keys.add(key)
                    except Exception:                  # noqa: BLE001 - never abort on one row
                        malformed += 1
        except OSError as exc:
            raise RuntimeError(f"processed-file tracking CSV could not be read "
                               f"({self.csv_path}): {exc}") from exc

        if malformed:
            logger.warning("%d malformed row(s) skipped in %s", malformed, self.csv_path)
        if duplicates:
            logger.info("%d duplicate row(s) in %s collapsed to a single entry",
                        duplicates, self.csv_path)
        logger.info("processed-file tracking: %d file(s) already processed", len(keys))
        self._keys = keys
        return keys

    def contains(self, discovered: DiscoveredFile) -> bool:
        return discovered.key(self.key_mode) in self.load_keys()

    # -- writing ----------------------------------------------------------- #

    def append_batch(self,
                     files: Iterable[DiscoveredFile],
                     batch_id: str,
                     run_id: str,
                     records: Optional[int] = None,
                     status: str = SUCCESS_STATUS,
                     processed_date: Optional[datetime] = None) -> int:
        """
        Append one row per file. Returns the number of rows written.

        Called only after Greenplum has confirmed the batch, so a row in this
        file always means "this file's data is committed in Greenplum".
        """
        files = list(files)
        if not files:
            return 0
        self.ensure_file()
        stamp = (processed_date or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

        try:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    writer = csv.writer(handle)
                    for discovered in files:
                        writer.writerow([
                            os.path.basename(discovered.path),
                            discovered.path,
                            discovered.key(self.key_mode),
                            discovered.atm_no,
                            discovered.size,
                            datetime.fromtimestamp(discovered.mtime).strftime("%Y-%m-%d %H:%M:%S"),
                            stamp,
                            batch_id,
                            run_id,
                            "" if records is None else records,
                            status,
                        ])
                    handle.flush()
                    os.fsync(handle.fileno())
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError as exc:
            raise RuntimeError(f"could not append to the processed-file tracking CSV "
                               f"({self.csv_path}): {exc}") from exc

        if self._keys is not None and status == SUCCESS_STATUS:
            self._keys.update(discovered.key(self.key_mode) for discovered in files)
        logger.info("processed-file tracking: %d file(s) recorded as %s for %s",
                    len(files), status, batch_id)
        return len(files)


# --------------------------------------------------------------------------- #
# Pending (in-flight) batch markers
# --------------------------------------------------------------------------- #


class PendingBatchStore:
    """
    Crash-safe markers for batches whose Greenplum load has been started.

    Written before the load, deleted after the tracking CSV has been updated.
    Anything left behind is an in-flight batch from a previous run and is
    resolved by :meth:`etl_runner.AtmEjournalEtl._recover_pending_batches`.
    """

    def __init__(self, pending_dir: str):
        self.pending_dir = pending_dir

    def path_for(self, batch_id: str) -> str:
        return os.path.join(self.pending_dir, f"{batch_id}.json")

    def write(self,
              batch_id: str,
              run_id: str,
              files: Iterable[DiscoveredFile],
              parquet_path: str,
              record_count: Optional[int] = None,
              extra: Optional[Dict[str, Any]] = None) -> str:
        os.makedirs(self.pending_dir, exist_ok=True)
        payload = {
            "batch_id": batch_id,
            "run_id": run_id,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "parquet_path": parquet_path,
            "record_count": record_count,
            "status": PENDING_STATUS,
            "files": [discovered.as_dict() for discovered in files],
        }
        payload.update(extra or {})
        path = self.path_for(batch_id)
        temporary = f"{path}.tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)                    # atomic
        logger.debug("pending marker written for %s: %s", batch_id, path)
        return path

    def list_pending(self) -> List[Dict[str, Any]]:
        if not os.path.isdir(self.pending_dir):
            return []
        markers: List[Dict[str, Any]] = []
        for name in sorted(os.listdir(self.pending_dir)):
            if not name.endswith(".json"):
                continue
            path = os.path.join(self.pending_dir, name)
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                payload["_marker_path"] = path
                markers.append(payload)
            except (OSError, ValueError) as exc:
                logger.error("unreadable pending marker %s: %s - it is left in place for "
                             "manual inspection", path, exc)
        return markers

    def remove(self, batch_id: str) -> None:
        path = self.path_for(batch_id)
        try:
            os.remove(path)
            logger.debug("pending marker cleared for %s", batch_id)
        except FileNotFoundError:
            pass
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                logger.warning("could not remove pending marker %s: %s", path, exc)

    @staticmethod
    def files_from_marker(payload: Dict[str, Any]) -> List[DiscoveredFile]:
        files: List[DiscoveredFile] = []
        for item in payload.get("files", []):
            try:
                files.append(DiscoveredFile(
                    path=item["path"],
                    relative_path=item["relative_path"],
                    atm_no=item.get("atm_no", ""),
                    size=int(item.get("size", 0)),
                    mtime=float(item.get("mtime", 0.0)),
                ))
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("skipping malformed file entry in pending marker: %s", exc)
        return files
