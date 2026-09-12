"""
ATM E-Journal Withdrawal Parser
===============================

Parses ATM electronic journal (e-journal) text files and extracts CASH WITHDRAWAL
transactions into a single tidy pandas DataFrame (one logical withdrawal = one row).

Expected input layout
---------------------
    ATM_EJOURNALS/
      ATM001/                      <- folder name is used as ATM_NO
        EJOURNAL_10092026_00.TXT
        EJOURNAL_11092026_00.TXT
      ATM002/
        ...

Usage
-----
    from atm_ejournal_parser import process_all_atms

    df = process_all_atms("ATM_EJOURNALS")
    print(df.head())
    print(df.attrs["stats"])          # audit counters
    df.attrs["unparsed"]              # DataFrame of low-confidence records

Journal grammar this parser is built around (derived from real files)
--------------------------------------------------------------------
Every log line:
    [DDMMYYYY HHMMSS mmm][DEV][LVL]> <message>

Card session (one customer at the ATM):
    ===================== Trx Started =====================
        -----Card Number : 539157******0717
        #SESSION-START#
        #TRANSACTION-START#              <- one logical transaction
            -Amount Requsted --------------------
            ---Amount        : 50000     <- amount keyed by customer
            -Cash Withdraw Initiated -------------
            -----Amount : 50000
            ----AUX NO : 2868 :xx:A002301120260910064244-02
            -----Withdraw Status : OK    <- OK | Failed
            -----Account         : 539157XX..XX0717
            -----Response        : 000
            -----Trace ID        : 559198
            -----Aux No          : A002301120260910064244-02
            ---Cash Withdraw Initiated Completed     (or ...Initiated Fail)
            -Dispense Command Executed / ---Dispense Succeeded
            ---Present Succeeded / ---Cash Has Taken
        #TRANSACTION-END#
    ============== Trx End:CardRemoved:9/10/2026 6:43:04 AM=============

Key observations encoded below:
* The withdrawal record boundary is the "Cash Withdraw Initiated" block, which
  terminates at "Cash Withdraw Initiated Completed" / "... Fail".
* A #TRANSACTION-START#/#TRANSACTION-END# block holds at most one withdrawal
  attempt, so retries appear as consecutive transaction blocks inside one card
  session.
* Failed attempts leave the Account field blank; the masked card number carried
  by the session is the reliable customer key, so it is used for grouping.
* The note breakdown that was actually paid out is the denomination table inside
  the "Dispense Command Executed" block (CU / TYP / VALUE / NUM). The earlier
  "Denominate Execution Completed" table is only the mix the ATM *planned* and
  frequently differs, so it is kept separately as PLANNED_DENOMINATION.
* A SUCCESS is terminal: cash was dispensed and taken. Repeated successes for
  the same card/amount are genuinely separate withdrawals and are never merged.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional

import pandas as pd

logger = logging.getLogger("atm_ejournal")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

#: Extensions recognised without inspecting the file.
JOURNAL_EXTENSIONS = (".txt", ".log", ".jrn", ".dat", ".ej", ".ejn", ".jnl", ".prn")

#: Extensions never treated as journals, whatever their content.
SKIP_EXTENSIONS = (".zip", ".gz", ".7z", ".rar", ".xlsx", ".xls", ".csv", ".pdf",
                   ".docx", ".png", ".jpg", ".jpeg", ".db", ".bak", ".exe", ".ipynb")

#: When True, a file with an unknown extension (or none at all) is opened and
#: checked for journal-style lines instead of being skipped.
SNIFF_UNKNOWN_EXTENSIONS = True

#: Lines read when sniffing an unknown file.
SNIFF_LINES = 200

#: How many log lines after "Cash Withdraw Initiated" we scan for its fields.
WITHDRAW_BLOCK_LOOKAHEAD = 80

#: How many lines after the withdraw block we scan for dispense / cash-taken.
OUTCOME_LOOKAHEAD = 200

FINAL_COLUMNS = [
    "ATM_NO",
    "TRANSACTION_DATETIME",
    "DATE",
    "TIME",
    "ACCOUNT_NO",
    "CARD_NO",
    "AMOUNT",
    "REQUESTED_AMOUNT",
    "CURRENCY",
    "STATUS",
    "RESPONSE_CODE",
    "TRANSACTION_REF",
    "TRACE_ID",
    "TERMINAL_ID",
    "CARD_SCHEME",
    "DISPENSE_RESULT",
    "DENOMINATION",
    "NOTES_COUNT",
    "DENOM_AMOUNT",
    "CASH_TAKEN",
    "TRX_ERROR",
    "ATTEMPT_NO",
    "ATTEMPT_COUNT",
    "IS_RETRY",
    "SESSION_ID",
    "TXN_SEQ",
    "SOURCE_FILE",
    "SOURCE_LINE",
]

# --------------------------------------------------------------------------- #
# Regular expressions (pattern-driven, never position-driven)
# --------------------------------------------------------------------------- #

LINE_RE = re.compile(
    r"^\[(?P<date>\d{8})\s+(?P<time>\d{6})\s+(?P<ms>\d{1,3})\]"
    r"\[(?P<dev>[^\]]*)\]\[(?P<level>[^\]]*)\]>\s?(?P<msg>.*)$"
)

# Leading dashes are decoration and vary between firmware versions -> strip them.
DASH_RE = re.compile(r"^[-=\s]+|[-=\s]+$")

SESSION_START_RE = re.compile(r"Trx\s+Started", re.I)
SESSION_END_RE = re.compile(r"Trx\s+End\s*:\s*CardRemoved\s*:\s*(?P<stamp>[^=]+)", re.I)
TXN_START_RE = re.compile(r"#TRANSACTION-START#", re.I)
TXN_END_RE = re.compile(r"#TRANSACTION-END#", re.I)

CARD_NO_RE = re.compile(r"Card\s*Number\s*:\s*(?P<card>[0-9X*.]{6,})", re.I)
TERMINAL_RE = re.compile(r"Terminal\s*ID\s*:\s*(?P<tid>\S+)", re.I)
APP_NAME_RE = re.compile(r"APP\s*NAME\s*:\s*(?P<app>.+)", re.I)
AMOUNT_REQUESTED_RE = re.compile(r"Amount\s+Requsted|Amount\s+Requested", re.I)
FAST_CASH_RE = re.compile(r"Fast\s*Cash", re.I)

WITHDRAW_START_RE = re.compile(r"^Cash\s+Withdraw\s+Initiated\s*$", re.I)
WITHDRAW_OK_RE = re.compile(r"^Cash\s+Withdraw\s+Initiated\s+Completed", re.I)
WITHDRAW_FAIL_RE = re.compile(r"^Cash\s+Withdraw\s+Initiated\s+(Fail|Failed)", re.I)

FIELD_RE = re.compile(r"^(?P<key>[A-Za-z][A-Za-z /#.]*?)\s*:\s*(?P<val>.*)$")
AUX_NO_RE = re.compile(r"AUX\s*NO\s*:\s*(?P<seq>\d+)\s*:[^:]*:\s*(?P<aux>\S+)", re.I)
TRX_ERROR_RE = re.compile(r"Trx\s*Error\s*:\s*(?P<code>\w+)", re.I)

DISPENSE_CMD_RE = re.compile(r"Dispense\s+Command\s+Executed", re.I)
DISPENSE_OK_RE = re.compile(r"Dispense\s+Succeeded", re.I)
DISPENSE_FAIL_RE = re.compile(r"Dispense\s+(Failed|Fail)", re.I)
CASH_TAKEN_RE = re.compile(r"Cash\s+Has\s+Taken|Items\s+Taken", re.I)
CASH_NOT_TAKEN_RE = re.compile(r"Cash\s+Not\s+Taken|Retract", re.I)

NUMERIC_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")

# Denomination table emitted by the dispenser:
#     -----CU  TYP  VALUE   NUM
#     -----02  RCY  005000  010      -> ten 5,000 notes
DENOM_ROW_RE = re.compile(r"^(?P<cu>\d{1,3})\s+(?P<typ>[A-Z]{2,4})\s+"
                          r"(?P<value>\d{2,8})\s+(?P<num>\d{1,4})$")
# Compact variant used by some firmware: Denom : 000100-000003:005000-000005
DENOM_COMPACT_RE = re.compile(r"(?<!\d)(?P<value>\d{3,8})\s*-\s*(?P<num>\d{1,6})(?!\d)")
DENOM_LINE_RE = re.compile(r"^Denom\s*:", re.I)
# Per-note-type triplet variant: Denom : 5000 / QTY : 5 / Amount : 25000
DENOM_QTY_RE = re.compile(r"^(?P<key>Denom|QTY|Currency\s*ID|Amount)\s*:\s*(?P<val>.*)$", re.I)
DENOMINATE_DONE_RE = re.compile(r"Denominate\s+Execution\s+Completed", re.I)
MIX_RE = re.compile(r"Mix(\s*Number)?\s*:\s*(?P<mix>\S+)", re.I)
DENOM_STOP_RE = re.compile(
    r"Dispense\s+Succeeded|Dispense\s+Fail|Present\s|Cash\s+Withdraw|#TRANSACTION-|"
    r"Trx\s+End|Trx\s+Started|CASH\s+UNIT\s+INFO|Send\s+EMV|Online\s+Requested",
    re.I,
)

SUCCESS_TOKENS = {"ok", "success", "successful", "approved", "completed", "suc"}
FAILURE_TOKENS = {"fail", "failed", "failure", "declined", "error", "cancelled", "canceled"}


# --------------------------------------------------------------------------- #
# Data containers
# --------------------------------------------------------------------------- #


@dataclass
class ParseStats:
    """Audit counters so no transaction is ever silently lost."""

    atm_folders_processed: int = 0
    atm_folders_without_files: int = 0
    files_processed: int = 0
    files_failed: int = 0
    lines_read: int = 0
    lines_unrecognised: int = 0
    card_sessions: int = 0
    withdrawal_records_detected: int = 0
    successful_withdrawals: int = 0
    failed_withdrawals: int = 0
    unknown_status_withdrawals: int = 0
    withdrawals_with_denomination: int = 0
    successful_without_denomination: int = 0
    denomination_amount_mismatches: int = 0
    removed_repeat_attempts: int = 0
    records_unparsed: int = 0

    def as_dict(self) -> Dict[str, int]:
        return asdict(self)


@dataclass
class LogLine:
    """One structured journal line."""

    lineno: int
    timestamp: Optional[datetime]
    device: str
    level: str
    msg: str          # message with decorative dashes stripped
    raw: str


@dataclass
class SessionContext:
    """State carried across a customer's card session."""

    session_id: str
    card_no: Optional[str] = None
    terminal_id: Optional[str] = None
    card_scheme: Optional[str] = None
    requested_amount: Optional[float] = None
    currency: Optional[str] = None
    fast_cash: bool = False
    txn_seq: int = 0
    implicit: bool = False
    extras: Dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Low-level helpers
# --------------------------------------------------------------------------- #


def _clean(text: str) -> str:
    """Collapse whitespace and strip decorative dashes/equals."""
    return DASH_RE.sub("", text.replace("\x00", "").strip())


def _to_number(value: Optional[str]) -> Optional[float]:
    """Parse an amount-like token into a float; returns None when not numeric."""
    if value is None:
        return None
    match = NUMERIC_RE.search(str(value))
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _parse_stamp(date_token: str, time_token: str, ms_token: str) -> Optional[datetime]:
    """
    Build a datetime from the journal header stamp.

    The header is DDMMYYYY HHMMSS mmm (confirmed against the human readable
    'Trx End:CardRemoved:<M/D/YYYY h:mm:ss AM>' footer in the same files).
    A YYYYMMDD fallback is attempted for firmware that writes ISO-ish stamps.
    """
    for fmt in ("%d%m%Y", "%Y%m%d"):
        try:
            day = datetime.strptime(date_token, fmt)
        except ValueError:
            continue
        try:
            hh, mm, ss = int(time_token[:2]), int(time_token[2:4]), int(time_token[4:6])
            micro = int(str(ms_token).ljust(3, "0")[:3]) * 1000
            return day.replace(hour=hh, minute=mm, second=ss, microsecond=micro)
        except ValueError:
            return day
    return None


def _field(msg: str) -> Optional[tuple]:
    """Split a '<Key> : <Value>' message into (key_lower, value)."""
    match = FIELD_RE.match(msg)
    if not match:
        return None
    key = re.sub(r"\s+", " ", match.group("key")).strip().lower()
    return key, match.group("val").strip()


def extract_denominations(buffer: List["LogLine"], start: int, end: int) -> Dict[int, int]:
    """
    Read a note-denomination breakdown out of the lines following ``start``.

    Handles the three layouts seen in the wild:

    * dispenser table   ``01  RCY  005000  010``      -> {5000: 10}
    * compact string    ``Denom : 000100-000003:005000-000005``
    * per-type triplet  ``Denom : 5000`` + ``QTY : 5``

    Returns ``{note_value: note_count}`` with zero-count rows dropped.
    """
    counts: Dict[int, int] = {}
    pending_value: Optional[int] = None

    for index in range(start + 1, min(end, len(buffer))):
        msg = buffer[index].msg
        if not msg:
            continue
        if DENOM_STOP_RE.search(msg):
            break

        row = DENOM_ROW_RE.match(msg)
        if row:
            value, num = int(row.group("value")), int(row.group("num"))
            if value > 0 and num > 0:
                counts[value] = counts.get(value, 0) + num
            continue

        if DENOM_LINE_RE.match(msg):
            pairs = DENOM_COMPACT_RE.findall(msg)
            if pairs:
                for value_token, num_token in pairs:
                    value, num = int(value_token), int(num_token)
                    if value > 0 and num > 0:
                        counts[value] = counts.get(value, 0) + num
                pending_value = None
                continue

        triplet = DENOM_QTY_RE.match(msg)
        if triplet:
            key = triplet.group("key").lower().replace(" ", "")
            value = _to_number(triplet.group("val"))
            if key == "denom" and value:
                pending_value = int(value)
            elif key == "qty" and pending_value and value:
                counts[pending_value] = counts.get(pending_value, 0) + int(value)
                pending_value = None

    return counts


def format_denomination(counts: Optional[Dict[int, int]]) -> Optional[str]:
    """Render {5000: 9, 1000: 3} as '5000x9 + 1000x3' (highest note first)."""
    if not counts:
        return None
    return " + ".join(f"{value}x{count}"
                      for value, count in sorted(counts.items(), reverse=True))


def denomination_value(counts: Optional[Dict[int, int]]) -> Optional[float]:
    """Total cash value implied by a note breakdown."""
    if not counts:
        return None
    return float(sum(value * count for value, count in counts.items()))


def determine_status(withdraw_status: Optional[str],
                     terminator: Optional[str],
                     response_code: Optional[str]) -> str:
    """
    Standardise a withdrawal outcome to SUCCESS / FAILED / UNKNOWN.

    Preference order: explicit 'Withdraw Status' field, then the block
    terminator line, then the host response code ('000' == approved).
    """
    for token in (withdraw_status, terminator):
        if not token:
            continue
        low = token.strip().lower()
        if any(t in low for t in FAILURE_TOKENS):
            return "FAILED"
        if any(low == t or low.startswith(t) for t in SUCCESS_TOKENS):
            return "SUCCESS"
    if response_code:
        return "SUCCESS" if str(response_code).strip() in {"000", "0", "00"} else "FAILED"
    return "UNKNOWN"


# --------------------------------------------------------------------------- #
# 1. Reading
# --------------------------------------------------------------------------- #


def read_ejournal_file(path: str,
                       stats: Optional[ParseStats] = None,
                       encoding: str = "latin-1") -> Iterator[LogLine]:
    """
    Stream a journal file as structured :class:`LogLine` objects.

    Lines that do not match the header pattern (wrapped text, banners, blank
    lines) are appended to the previous line's message instead of being dropped,
    so multi-line values are preserved. Never raises on malformed content.
    """
    previous: Optional[LogLine] = None
    with open(path, "r", encoding=encoding, errors="replace", newline="") as handle:
        for lineno, raw in enumerate(handle, start=1):
            raw = raw.rstrip("\r\n")
            if stats:
                stats.lines_read += 1
            match = LINE_RE.match(raw)
            if not match:
                if raw.strip() and previous is not None:
                    previous.msg = f"{previous.msg} {_clean(raw)}".strip()
                elif raw.strip() and stats:
                    stats.lines_unrecognised += 1
                continue
            if previous is not None:
                yield previous
            previous = LogLine(
                lineno=lineno,
                timestamp=_parse_stamp(match.group("date"), match.group("time"), match.group("ms")),
                device=match.group("dev").strip(),
                level=match.group("level").strip(),
                msg=_clean(match.group("msg")),
                raw=raw,
            )
    if previous is not None:
        yield previous


# --------------------------------------------------------------------------- #
# 2. Session / transaction segmentation
# --------------------------------------------------------------------------- #


def extract_transaction_blocks(lines: Iterable[LogLine],
                               source_file: str,
                               stats: ParseStats) -> List[Dict[str, Any]]:
    """
    Walk a file once, tracking card-session context, and emit one raw record per
    'Cash Withdraw Initiated' block found.

    Session context (card number, terminal, scheme, keyed amount) lives outside
    the withdrawal block itself, which is why a single stateful pass is used
    rather than independent regex sweeps.
    """
    buffer: List[LogLine] = list(lines)
    records: List[Dict[str, Any]] = []

    session: Optional[SessionContext] = None
    session_counter = 0
    base = os.path.basename(source_file)

    def open_session(implicit: bool = False) -> SessionContext:
        nonlocal session_counter
        session_counter += 1
        stats.card_sessions += 1
        return SessionContext(session_id=f"{base}#S{session_counter:05d}", implicit=implicit)

    for index, line in enumerate(buffer):
        msg = line.msg
        if not msg:
            continue

        # ---- session boundaries -------------------------------------------
        if SESSION_START_RE.search(msg):
            session = open_session()
            continue
        if SESSION_END_RE.search(msg):
            session = None
            continue
        if TXN_START_RE.search(msg):
            if session is None:                       # journal starts mid-session
                session = open_session(implicit=True)
            session.txn_seq += 1
            session.requested_amount = None           # amount is re-keyed per txn
            session.fast_cash = False
            continue
        if TXN_END_RE.search(msg):
            continue

        # ---- session-level context ----------------------------------------
        card = CARD_NO_RE.search(msg)
        if card:
            if session is None:
                session = open_session(implicit=True)
            session.card_no = card.group("card").strip()
            continue

        terminal = TERMINAL_RE.search(msg)
        if terminal and session is not None:
            session.terminal_id = terminal.group("tid").strip()
            continue

        app = APP_NAME_RE.search(msg)
        if app and session is not None:
            session.card_scheme = app.group("app").strip()
            continue

        if FAST_CASH_RE.search(msg) and session is not None:
            session.fast_cash = True

        if DENOMINATE_DONE_RE.search(msg) and session is not None:
            # The mix the ATM *planned*; the dispenser may still pay a different
            # combination, so this is kept only for comparison.
            planned = extract_denominations(buffer, index, index + 20)
            if planned:
                session.extras["planned_denom"] = planned
            continue

        parsed = _field(msg)
        if parsed and session is not None:
            key, value = parsed
            if key == "amount" and session.requested_amount is None:
                # The customer-keyed amount, logged just after 'Amount Requsted'.
                session.requested_amount = _to_number(value)
            elif key in {"currency", "currency id"}:
                session.currency = value or None

        # ---- withdrawal block ---------------------------------------------
        if WITHDRAW_START_RE.match(msg):
            if session is None:
                session = open_session(implicit=True)
            record = parse_withdrawal(buffer, index, session, source_file, stats)
            if record:
                records.append(record)

    return records


# --------------------------------------------------------------------------- #
# 3. Withdrawal block parsing
# --------------------------------------------------------------------------- #


def parse_withdrawal(buffer: List[LogLine],
                     start: int,
                     session: SessionContext,
                     source_file: str,
                     stats: ParseStats) -> Optional[Dict[str, Any]]:
    """
    Parse one 'Cash Withdraw Initiated' block into a flat record dict.

    The block runs from the initiation line to its
    'Cash Withdraw Initiated Completed/Fail' terminator; dispense and
    cash-taken outcomes that follow inside the same transaction are attached
    as supplementary fields.
    """
    header = buffer[start]
    stats.withdrawal_records_detected += 1

    fields: Dict[str, str] = {}
    terminator: Optional[str] = None
    aux_no: Optional[str] = None
    aux_seq: Optional[str] = None
    end_index = start
    response_dt: Optional[datetime] = None

    limit = min(len(buffer), start + WITHDRAW_BLOCK_LOOKAHEAD)
    for index in range(start + 1, limit):
        line = buffer[index]
        msg = line.msg
        if not msg:
            continue

        if WITHDRAW_OK_RE.match(msg) or WITHDRAW_FAIL_RE.match(msg):
            terminator = msg
            end_index = index
            response_dt = line.timestamp
            break
        # A new block boundary means this withdrawal was never concluded.
        if (WITHDRAW_START_RE.match(msg) or TXN_END_RE.search(msg)
                or SESSION_END_RE.search(msg) or SESSION_START_RE.search(msg)):
            end_index = index - 1
            break

        aux = AUX_NO_RE.search(msg)
        if aux:
            aux_seq, aux_no = aux.group("seq"), aux.group("aux")
            continue

        parsed = _field(msg)
        if parsed:
            key, value = parsed
            if key not in fields or (not fields[key] and value):
                fields[key] = value
        end_index = index

    # ---- outcome after the host response (dispense / present / cash taken) --
    dispense_result: Optional[str] = None
    dispensed_amount: Optional[float] = None
    cash_taken: Optional[bool] = None
    trx_error: Optional[str] = None
    denom_counts: Dict[int, int] = {}
    mix_number: Optional[str] = None

    for index in range(end_index + 1, min(len(buffer), end_index + OUTCOME_LOOKAHEAD)):
        msg = buffer[index].msg
        if not msg:
            continue
        if WITHDRAW_START_RE.match(msg) or SESSION_END_RE.search(msg) or TXN_START_RE.search(msg):
            break
        if DISPENSE_CMD_RE.search(msg):
            # The note breakdown actually paid out lives in this block.
            found = extract_denominations(buffer, index, index + 40)
            for value, num in found.items():
                denom_counts[value] = denom_counts.get(value, 0) + num
            mix = MIX_RE.search(msg)
            if mix:
                mix_number = mix.group("mix")
        if DISPENSE_OK_RE.search(msg):
            dispense_result = "SUCCESS"
        elif DISPENSE_FAIL_RE.search(msg):
            dispense_result = "FAILED"
        elif CASH_TAKEN_RE.search(msg):
            cash_taken = True
        elif CASH_NOT_TAKEN_RE.search(msg):
            cash_taken = False if cash_taken is None else cash_taken
        error = TRX_ERROR_RE.search(msg)
        if error:
            trx_error = error.group("code")
        parsed = _field(msg)
        if parsed and parsed[0] == "amount" and dispensed_amount is None and dispense_result:
            dispensed_amount = _to_number(parsed[1])
        if parsed and parsed[0] in {"mix", "mix number"} and mix_number is None:
            mix_number = parsed[1]
        if TXN_END_RE.search(msg):
            break

    amount = _to_number(fields.get("amount"))
    if amount is None:
        amount = _to_number(fields.get("requestd amount") or fields.get("requested amount"))

    account = (fields.get("account") or fields.get("account number") or "").strip() or None
    status = determine_status(fields.get("withdraw status"), terminator, fields.get("response"))

    if denom_counts:
        stats.withdrawals_with_denomination += 1
        if amount is not None and abs(denomination_value(denom_counts) - amount) > 0.01:
            stats.denomination_amount_mismatches += 1
    elif status == "SUCCESS":
        stats.successful_without_denomination += 1

    if status == "SUCCESS":
        stats.successful_withdrawals += 1
    elif status == "FAILED":
        stats.failed_withdrawals += 1
    else:
        stats.unknown_status_withdrawals += 1

    record = {
        "ATM_NO": None,                       # filled in by process_atm_folder
        "TRANSACTION_DATETIME": header.timestamp,
        "RESPONSE_DATETIME": response_dt,
        "ACCOUNT_NO": account,
        "CARD_NO": session.card_no,
        "AMOUNT": amount,
        "REQUESTED_AMOUNT": session.requested_amount,
        "CURRENCY": fields.get("currency") or fields.get("currency id") or session.currency,
        "STATUS": status,
        "RESPONSE_CODE": (fields.get("response") or fields.get("response code") or "").strip() or None,
        "ACTION_CODE": (fields.get("action code") or "").strip() or None,
        "TRANSACTION_REF": aux_no or (fields.get("aux no") or "").strip() or None,
        "AUX_SEQ": aux_seq,
        "TRACE_ID": (fields.get("trace id") or "").strip() or None,
        "TERMINAL_ID": session.terminal_id,
        "CARD_SCHEME": session.card_scheme,
        "FAST_CASH": session.fast_cash,
        "DISPENSE_RESULT": dispense_result,
        "DISPENSED_AMOUNT": dispensed_amount,
        "DENOMINATION": format_denomination(denom_counts),
        "DENOM_BREAKDOWN": denom_counts or None,
        "NOTES_COUNT": int(sum(denom_counts.values())) if denom_counts else None,
        "DENOM_AMOUNT": denomination_value(denom_counts),
        "PLANNED_DENOMINATION": format_denomination(session.extras.get("planned_denom")),
        "MIX_NUMBER": mix_number,
        "CASH_TAKEN": cash_taken,
        "TRX_ERROR": trx_error,
        "SESSION_ID": session.session_id,
        "TXN_SEQ": session.txn_seq,
        "SOURCE_FILE": os.path.basename(source_file),
        "SOURCE_PATH": source_file,
        "SOURCE_LINE": header.lineno,
        "PARSE_CONFIDENT": bool(amount is not None and status != "UNKNOWN"),
    }

    if not record["PARSE_CONFIDENT"]:
        stats.records_unparsed += 1
        record["RAW_BLOCK"] = " | ".join(
            b.msg for b in buffer[start:end_index + 1] if b.msg
        )[:2000]

    return record


# Convenience wrappers kept as named units (useful for unit tests / reuse).
def extract_datetime(line: LogLine) -> Optional[datetime]:
    """Transaction datetime = timestamp of the line that opens the block."""
    return line.timestamp


def extract_account(fields: Dict[str, str]) -> Optional[str]:
    return (fields.get("account") or fields.get("account number") or "").strip() or None


def extract_amount(fields: Dict[str, str]) -> Optional[float]:
    return _to_number(fields.get("amount"))


# --------------------------------------------------------------------------- #
# 4. Repeated-attempt de-duplication
# --------------------------------------------------------------------------- #


def deduplicate_attempts(records: List[Dict[str, Any]],
                         stats: ParseStats,
                         keep_last_failure: bool = True,
                         link_failed_across_amounts: bool = False,
                         retry_window_seconds: int = 180) -> List[Dict[str, Any]]:
    """
    Collapse retries of the same logical withdrawal.

    A retry chain is a run of *consecutive* attempts inside one card session
    that share the same card (or account) and the same amount. A SUCCESS closes
    the chain, because cash left the machine — so three successful 2,500 LKR
    withdrawals on one card stay three rows, while FAIL/FAIL/FAIL/SUCCESS
    collapses to the first FAILED plus the final SUCCESS.

    Rules applied per chain:
      * keep the first failed attempt
      * keep the final successful attempt
      * drop intermediate failed attempts
      * all-failed chains keep the first and (optionally) the last failure

    Set ``link_failed_across_amounts=True`` to also chain a failed attempt to a
    following attempt on the same card within ``retry_window_seconds`` when the
    customer re-keys a *different* amount (e.g. 2,000 declined -> 1,500 taken).
    It is off by default because amount equality is the safer boundary between
    a retry and a second, genuinely separate withdrawal.
    """
    ordered = sorted(
        records,
        key=lambda r: (
            r.get("SESSION_ID") or "",
            r.get("TRANSACTION_DATETIME") or datetime.min,
            r.get("SOURCE_LINE") or 0,
        ),
    )

    kept: List[Dict[str, Any]] = []
    chain: List[Dict[str, Any]] = []

    def chain_key(rec: Dict[str, Any]):
        identity = rec.get("CARD_NO") or rec.get("ACCOUNT_NO") or rec.get("SESSION_ID")
        return rec.get("SESSION_ID"), identity, rec.get("AMOUNT")

    def flush(current: List[Dict[str, Any]]) -> None:
        if not current:
            return
        for position, rec in enumerate(current, start=1):
            rec["ATTEMPT_NO"] = position
            rec["ATTEMPT_COUNT"] = len(current)
            rec["IS_RETRY"] = len(current) > 1
        successes = [r for r in current if r["STATUS"] == "SUCCESS"]
        failures = [r for r in current if r["STATUS"] != "SUCCESS"]

        selected: List[Dict[str, Any]] = []
        if failures:
            selected.append(failures[0])
        if successes:
            selected.append(successes[-1])
        elif keep_last_failure and len(failures) > 1:
            selected.append(failures[-1])
        if not selected:
            selected = current[:1]

        seen_ids = {id(r) for r in selected}
        stats.removed_repeat_attempts += len(current) - len(seen_ids)
        kept.extend(sorted(selected, key=lambda r: r["ATTEMPT_NO"]))

    for rec in ordered:
        if not chain:
            chain = [rec]
            continue
        previous = chain[-1]
        same_group = chain_key(rec) == chain_key(previous)
        if not same_group and link_failed_across_amounts:
            same_customer = (
                rec.get("SESSION_ID") == previous.get("SESSION_ID")
                and (rec.get("CARD_NO") or rec.get("ACCOUNT_NO"))
                == (previous.get("CARD_NO") or previous.get("ACCOUNT_NO"))
            )
            gap = None
            if rec.get("TRANSACTION_DATETIME") and previous.get("TRANSACTION_DATETIME"):
                gap = (rec["TRANSACTION_DATETIME"] - previous["TRANSACTION_DATETIME"]).total_seconds()
            same_group = bool(
                same_customer
                and previous["STATUS"] != "SUCCESS"
                and gap is not None
                and 0 <= gap <= retry_window_seconds
            )
        chain_closed = any(r["STATUS"] == "SUCCESS" for r in chain)
        if same_group and not chain_closed:
            chain.append(rec)
        else:
            flush(chain)
            chain = [rec]
    flush(chain)

    return kept


# --------------------------------------------------------------------------- #
# 5. Folder orchestration
# --------------------------------------------------------------------------- #


def looks_like_journal(path: str, sniff_lines: int = SNIFF_LINES) -> bool:
    """Open a file and report whether its first lines carry journal headers."""
    try:
        with open(path, "r", encoding="latin-1", errors="replace") as handle:
            for count, line in enumerate(handle):
                if LINE_RE.match(line.rstrip("\r\n")):
                    return True
                if count >= sniff_lines:
                    break
    except OSError:
        return False
    return False


def _is_journal_file(path: str) -> bool:
    """
    Decide whether a file should be parsed.

    Known journal extensions are accepted outright. Anything else that is not on
    the skip list is opened and sniffed, so files named ``EJ_001``, ``*.001`` or
    with no extension at all are still picked up.
    """
    name = os.path.basename(path)
    if name.startswith(".") or name.startswith("~$"):
        return False
    lower = name.lower()
    if lower.endswith(SKIP_EXTENSIONS):
        return False
    if lower.endswith(JOURNAL_EXTENSIONS):
        return True
    return SNIFF_UNKNOWN_EXTENSIONS and looks_like_journal(path)


def find_journal_files(folder: str) -> List[str]:
    """Every journal file under ``folder``, recursively, symlinked dirs included."""
    found: List[str] = []
    for root, dirs, files in os.walk(folder, followlinks=True):
        dirs[:] = [d for d in sorted(dirs) if not d.startswith(".")]
        for name in sorted(files):
            path = os.path.join(root, name)
            if _is_journal_file(path):
                found.append(path)
    return found


def scan_input_tree(input_directory: str) -> pd.DataFrame:
    """
    Diagnostic: list every file under ``input_directory`` and say whether the
    parser will read it, without parsing anything.

    Columns: ATM_FOLDER, FILE, RELATIVE_PATH, SIZE_BYTES, WILL_PARSE, REASON.
    """
    rows: List[Dict[str, Any]] = []
    entries = sorted(e for e in os.listdir(input_directory)
                     if os.path.isdir(os.path.join(input_directory, e)))
    if not entries:
        entries = [""]

    for entry in entries:
        folder = os.path.join(input_directory, entry) if entry else input_directory
        seen = False
        for root, dirs, files in os.walk(folder, followlinks=True):
            dirs[:] = [d for d in sorted(dirs) if not d.startswith(".")]
            for name in sorted(files):
                seen = True
                path = os.path.join(root, name)
                lower = name.lower()
                will = _is_journal_file(path)
                if will:
                    reason = ("known extension" if lower.endswith(JOURNAL_EXTENSIONS)
                              else "sniffed: journal headers found")
                elif name.startswith(".") or name.startswith("~$"):
                    reason = "hidden / temp file"
                elif lower.endswith(SKIP_EXTENSIONS):
                    reason = "extension on skip list"
                else:
                    reason = "no journal headers in first lines"
                try:
                    size = os.path.getsize(path)
                except OSError:
                    size = -1
                rows.append({
                    "ATM_FOLDER": entry or os.path.basename(os.path.normpath(input_directory)),
                    "FILE": name,
                    "RELATIVE_PATH": os.path.relpath(path, input_directory),
                    "SIZE_BYTES": size,
                    "WILL_PARSE": will,
                    "REASON": reason,
                })
        if not seen:
            rows.append({
                "ATM_FOLDER": entry,
                "FILE": None,
                "RELATIVE_PATH": None,
                "SIZE_BYTES": 0,
                "WILL_PARSE": False,
                "REASON": "folder contains no files",
            })
    return pd.DataFrame(rows)


def process_atm_folder(folder: str,
                       atm_no: str,
                       stats: ParseStats,
                       **dedup_options: Any) -> List[Dict[str, Any]]:
    """Parse every journal file under one ATM folder (recursively)."""
    records: List[Dict[str, Any]] = []
    paths = find_journal_files(folder)

    if not paths:
        stats.atm_folders_without_files += 1
        logger.warning("ATM %s: no journal files found under %s", atm_no, folder)
        return []

    for path in paths:
        try:
            lines = read_ejournal_file(path, stats)
            file_records = extract_transaction_blocks(lines, path, stats)
            for rec in file_records:
                rec["ATM_NO"] = str(atm_no)
                # Namespace the session id so identical filenames in different
                # ATM folders can never collide in the combined DataFrame.
                rec["SESSION_ID"] = f"{atm_no}|{rec['SESSION_ID']}"
            records.extend(file_records)
            stats.files_processed += 1
            logger.info("ATM %s | %s -> %d withdrawal attempts",
                        atm_no, os.path.basename(path), len(file_records))
        except Exception as exc:                      # never abort the run
            stats.files_failed += 1
            logger.exception("Failed to parse %s: %s", path, exc)

    kept = deduplicate_attempts(records, stats, **dedup_options)
    logger.info("ATM %s: %d file(s), %d attempts -> %d rows",
                atm_no, len(paths), len(records), len(kept))
    return kept


def process_all_atms(input_directory: str,
                     verbose: bool = True,
                     **dedup_options: Any) -> pd.DataFrame:
    """
    Parse every ATM folder under ``input_directory`` and return the final
    withdrawal-level DataFrame (``df``).

    Audit counters are attached at ``df.attrs["stats"]`` and low-confidence
    records at ``df.attrs["unparsed"]``. Extra keyword arguments are forwarded
    to :func:`deduplicate_attempts` (``keep_last_failure``,
    ``link_failed_across_amounts``, ``retry_window_seconds``).
    """
    if verbose and not logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    stats = ParseStats()
    all_records: List[Dict[str, Any]] = []

    if not os.path.isdir(input_directory):
        raise NotADirectoryError(input_directory)

    entries = sorted(e for e in os.listdir(input_directory)
                     if os.path.isdir(os.path.join(input_directory, e)))

    # Tolerate a flat directory of journal files (no per-ATM subfolders).
    if not entries:
        entries = [""]

    for entry in entries:
        folder = os.path.join(input_directory, entry) if entry else input_directory
        atm_no = entry or os.path.basename(os.path.normpath(input_directory))
        stats.atm_folders_processed += 1
        all_records.extend(process_atm_folder(folder, atm_no, stats, **dedup_options))

    if stats.atm_folders_without_files:
        logger.warning("%d of %d ATM folder(s) produced no journal files - "
                       "run scan_input_tree(input_directory) to see why",
                       stats.atm_folders_without_files, stats.atm_folders_processed)

    df = _build_dataframe(all_records)
    df.attrs["stats"] = stats.as_dict()
    df.attrs["unparsed"] = _build_unparsed(all_records)

    if verbose:
        by_atm = {}
        for rec in all_records:
            by_atm[rec["ATM_NO"]] = by_atm.get(rec["ATM_NO"], 0) + 1
        logger.info("Rows per ATM: %s", by_atm)
        logger.info("Summary: %s", stats.as_dict())
    return df


# --------------------------------------------------------------------------- #
# 6. DataFrame assembly and typing
# --------------------------------------------------------------------------- #


def _build_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    df = pd.DataFrame(records)
    df["TRANSACTION_DATETIME"] = pd.to_datetime(df["TRANSACTION_DATETIME"], errors="coerce")
    df["RESPONSE_DATETIME"] = pd.to_datetime(df.get("RESPONSE_DATETIME"), errors="coerce")
    df["DATE"] = df["TRANSACTION_DATETIME"].dt.date
    df["TIME"] = df["TRANSACTION_DATETIME"].dt.time

    df = _expand_denomination_columns(df)

    for col in ("AMOUNT", "REQUESTED_AMOUNT", "DISPENSED_AMOUNT", "DENOM_AMOUNT"):
        df[col] = pd.to_numeric(df.get(col), errors="coerce")

    for col in ("ATM_NO", "ACCOUNT_NO", "CARD_NO", "TRANSACTION_REF", "TRACE_ID",
                "RESPONSE_CODE", "TERMINAL_ID", "CARD_SCHEME", "TRX_ERROR",
                "SESSION_ID", "SOURCE_FILE", "AUX_SEQ", "ACTION_CODE"):
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip().replace({"": pd.NA})

    df["STATUS"] = (df["STATUS"].astype("string").str.upper()
                    .where(df["STATUS"].isin(["SUCCESS", "FAILED", "UNKNOWN"]), "UNKNOWN"))

    note_cols = df.attrs.get("note_columns", [])
    head = FINAL_COLUMNS[:FINAL_COLUMNS.index("DENOM_AMOUNT") + 1]
    tail = FINAL_COLUMNS[FINAL_COLUMNS.index("DENOM_AMOUNT") + 1:]
    ordered = head + note_cols + ["DENOM_MATCHES_AMOUNT"] + tail
    ordered += [c for c in df.columns if c not in ordered]
    note_columns = df.attrs.get("note_columns", [])
    df = df[[c for c in ordered if c in df.columns]]
    df.attrs["note_columns"] = note_columns
    return df.sort_values(["ATM_NO", "TRANSACTION_DATETIME"]).reset_index(drop=True)


def _expand_denomination_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn DENOM_BREAKDOWN ({5000: 9, 1000: 3}) into one NOTES_<value> column per
    note denomination seen anywhere in the run, and flag value mismatches.
    """
    if "DENOM_BREAKDOWN" not in df.columns:
        return df

    values = sorted({int(v)
                     for breakdown in df["DENOM_BREAKDOWN"].dropna()
                     for v in breakdown}, reverse=True)

    for value in values:
        df[f"NOTES_{value}"] = df["DENOM_BREAKDOWN"].map(
            lambda b, v=value: b.get(v, 0) if isinstance(b, dict) else pd.NA
        ).astype("Int64")

    df["NOTES_COUNT"] = pd.to_numeric(df.get("NOTES_COUNT"), errors="coerce").astype("Int64")
    amount = pd.to_numeric(df.get("AMOUNT"), errors="coerce")
    denom_amount = pd.to_numeric(df.get("DENOM_AMOUNT"), errors="coerce")
    df["DENOM_MATCHES_AMOUNT"] = (denom_amount - amount).abs().le(0.01).where(
        denom_amount.notna(), pd.NA)

    df.attrs["note_columns"] = [f"NOTES_{v}" for v in values]
    return df


def explode_denominations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Long-format view: one row per (withdrawal, note value), with note counts and
    cash value. Handy for cassette forecasting and note-usage analysis.

    Columns: ATM_NO, TRANSACTION_DATETIME, TRANSACTION_REF, STATUS,
    NOTE_VALUE, NOTE_COUNT, NOTE_AMOUNT.
    """
    rows: List[Dict[str, Any]] = []
    for record in df.to_dict("records"):
        breakdown = record.get("DENOM_BREAKDOWN")
        if not isinstance(breakdown, dict):
            continue
        for value, count in sorted(breakdown.items(), reverse=True):
            rows.append({
                "ATM_NO": record.get("ATM_NO"),
                "TRANSACTION_DATETIME": record.get("TRANSACTION_DATETIME"),
                "TRANSACTION_REF": record.get("TRANSACTION_REF"),
                "STATUS": record.get("STATUS"),
                "NOTE_VALUE": int(value),
                "NOTE_COUNT": int(count),
                "NOTE_AMOUNT": int(value) * int(count),
            })
    return pd.DataFrame(rows, columns=["ATM_NO", "TRANSACTION_DATETIME", "TRANSACTION_REF",
                                       "STATUS", "NOTE_VALUE", "NOTE_COUNT", "NOTE_AMOUNT"])


def _build_unparsed(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = [r for r in records if not r.get("PARSE_CONFIDENT", True)]
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# --------------------------------------------------------------------------- #
# 7. Optional exports (DataFrame stays the primary output)
# --------------------------------------------------------------------------- #


def export_csv(df: pd.DataFrame, path: str) -> str:
    df.to_csv(path, index=False)
    return path


def export_excel(df: pd.DataFrame, path: str) -> str:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="withdrawals", index=False)
        stats = df.attrs.get("stats")
        if stats:
            pd.DataFrame(stats.items(), columns=["METRIC", "VALUE"]).to_excel(
                writer, sheet_name="audit", index=False)
    return path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse ATM e-journals into a withdrawal dataset.")
    parser.add_argument("input_directory")
    parser.add_argument("--csv", help="optional CSV export path")
    args = parser.parse_args()

    df = process_all_atms(args.input_directory)
    print(df.head(20).to_string())
    print("\nAudit:", df.attrs["stats"])
    if args.csv:
        print("Written:", export_csv(df, args.csv))
