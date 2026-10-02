"""
ATM E-Journal Withdrawal + Deposit Parser
=========================================

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
    from atm_ejournal_parser import process_all_atms, process_all_atms_deposits

    df = process_all_atms("ATM_EJOURNALS")            # withdrawals
    dep = process_all_atms_deposits("ATM_EJOURNALS")  # cash deposits
    print(df.attrs["stats"])          # audit counters
    df.attrs["unparsed"]              # DataFrame of low-confidence records

    # one file at a time (what the batched Greenplum loader uses):
    from atm_ejournal_parser import process_single_file, process_single_deposit_file

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

Transaction types captured
--------------------------
Withdrawal pass (:func:`process_all_atms`, TRANSACTION_TYPE):
    WITHDRAWAL            "Cash Withdraw Initiated" - card withdrawal
    FAST_CASH             "Fast Cash" - preset-amount card withdrawal
    IWALLET_WITHDRAWAL    "iWallet Cash Withdraw" - cardless wallet withdrawal

Deposit pass (:func:`process_all_atms_deposits`, DEPOSIT_TYPE):
    CARD                  "Cash Deposit Request" inside a card session
    CARDLESS              "Cardless Cash Deposit Request"
    BILL_PAYMENT          cash accepted and paid to a biller instead of an
                          account ("Cardless BillPayment - Biller Verification"
                          + "Cardless Bill Payment Confirm Request")

Amount vs. denomination
-----------------------
Every row carries both the amount the host recorded (AMOUNT) and the value of
the notes the machine actually handled (DENOM_AMOUNT), plus:

    AMOUNT_SOURCE         where AMOUNT came from: REQUEST (as logged),
                          REQUEST_SCALED (logged in minor units, divided),
                          DENOMINATION (no amount logged - the notes are the
                          amount, which is how a bill payment is recorded)
    DENOM_AMOUNT_DIFF     DENOM_AMOUNT - AMOUNT (0.0 when they agree)
    DENOM_MATCHES_AMOUNT  the same test as a boolean

Amounts logged in minor units (``Trx Amount : 190000`` for 1,900.00, every
deposit "Amount") are only rescaled when the note breakdown confirms the scale,
so a firmware version that logs rupees is not divided by 100 as well. When the
two still disagree the row is kept, the difference is reported in
DENOM_AMOUNT_DIFF and the mismatch is counted in the audit stats - nothing is
silently "corrected".
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
    "TRANSACTION_TYPE",
    "ACCOUNT_NO",
    "CARD_NO",
    "AMOUNT",
    "AMOUNT_RAW",
    "REQUESTED_AMOUNT",
    "AMOUNT_SOURCE",
    "CURRENCY",
    "STATUS",
    "RESPONSE_CODE",
    "TRANSACTION_REF",
    "TRACE_ID",
    "TERMINAL_ID",
    "CARD_SCHEME",
    "WALLET_ID",
    "BANK_CODE",
    "RET_REF_NO",
    "FEE",
    "DISPENSE_RESULT",
    "DENOMINATION",
    "NOTES_COUNT",
    "DENOM_AMOUNT",
    "DENOM_AMOUNT_DIFF",
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
#: A cardless leg (wallet withdrawal, cardless deposit) has no card footer - it
#: closes on "Close Session" / #SESSION-END# instead. Honouring both keeps each
#: cardless transaction in its own session instead of one session per file.
SESSION_CLOSE_RE = re.compile(r"^Close\s+Session\b|#SESSION-END#", re.I)
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

#: Fast Cash is a withdrawal with a preset amount. The CDM logs it under its
#: own anchor instead of "Cash Withdraw Initiated", so it needs its own block
#: pair; the record it produces is an ordinary withdrawal row carrying
#: TRANSACTION_TYPE = FAST_CASH.
FAST_CASH_START_RE = re.compile(r"^Fast\s*Cash$", re.I)
FAST_CASH_FAIL_RE = re.compile(
    r"^Fast\s*Cash\s+(Fail|Failed|Completed\s+(NG|Fail|Failed|Error|Declined))", re.I)
FAST_CASH_OK_RE = re.compile(r"^Fast\s*Cash\s+Completed", re.I)

#: Set to False to go back to withdrawal-only capture.
CAPTURE_FAST_CASH = True

# --- iWallet cash withdrawal ----------------------------------------------- #
#
# A wallet withdrawal is cardless: there is no "Trx Started", no card number and
# no "Cash Withdraw Initiated". The host leg is its own block:
#
#     -iWallet Cash Withdraw
#     ---Account Number      :                 <- empty: the wallet is the identity
#     ---Trx Amount          : 190000          <- MINOR UNITS (1,900.00)
#     ---Bank Code           : 6278
#     ---Ret Ref No          : 723314775978
#     ---Wallet ID           : SRANJALA
#     ---Aux Number          : A000013120260821135102-01
#     -iWallet Cash Withdraw Successful        <- terminator carries the outcome
#     ---RESP_CODE           : 000
#     ---TRACE_NO            : 592498
#     -Dispense Command Executed -----------   <- notes actually paid out
#     ---Dispense Succeeded / ---Cash Has Taken
#
# It produces an ordinary withdrawal row with TRANSACTION_TYPE =
# IWALLET_WITHDRAWAL, so downstream reporting needs no new table.
IWALLET_START_RE = re.compile(r"^iWallet\s+Cash\s+Withdraw$", re.I)
IWALLET_OK_RE = re.compile(
    r"^iWallet\s+Cash\s+Withdraw\s+(Successful|Success|Completed|OK)", re.I)
IWALLET_FAIL_RE = re.compile(
    r"^iWallet\s+Cash\s+Withdraw\s+(Fail|Failed|Unsuccessful|Declined|Error|NG|Rejected)",
    re.I)

#: Set to False to go back to card-withdrawal-only capture.
CAPTURE_IWALLET = True

#: Lines scanned after the iWallet terminator for the host response fields
#: (AUX_NO / RESP_CODE / ACT_CODE / TRACE_NO / balances).
IWALLET_TRAILING_LINES = 12


def _is_block_start(msg: str) -> bool:
    """True for any line that opens a withdrawal-type block."""
    return bool(WITHDRAW_START_RE.match(msg)
                or (CAPTURE_FAST_CASH and FAST_CASH_START_RE.match(msg))
                or (CAPTURE_IWALLET and IWALLET_START_RE.match(msg)))

# Keys may carry underscores and hyphens (RESP_CODE, TRACE_NO, AVAIL_BAL_DR_CR,
# Ret Ref No) - the wallet legs log in that style.
FIELD_RE = re.compile(r"^(?P<key>[A-Za-z][A-Za-z0-9 /#._-]*?)\s*:\s*(?P<val>.*)$")
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
    fast_cash_records_detected: int = 0
    iwallet_records_detected: int = 0
    successful_withdrawals: int = 0
    failed_withdrawals: int = 0
    unknown_status_withdrawals: int = 0
    withdrawals_with_denomination: int = 0
    successful_without_denomination: int = 0
    denomination_amount_mismatches: int = 0
    amount_scale_corrections: int = 0
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


#: Amounts logged in minor units are 100x the rupee value.
MINOR_UNIT_DIVISOR = 100.0

#: Two amounts count as equal below this absolute difference.
AMOUNT_TOLERANCE = 0.01


def reconcile_amount(raw: Optional[float],
                     note_total: Optional[float],
                     minor_units: bool = False) -> tuple:
    """
    Decide the rupee value of a logged amount, and say where it came from.

    Returns ``(amount, source)`` where ``source`` is one of:

    ``REQUEST``
        the amount as logged (ordinary withdrawals, firmware that logs rupees);
    ``REQUEST_SCALED``
        the amount divided by :data:`MINOR_UNIT_DIVISOR` (the deposit and
        iWallet legs log cents: ``Trx Amount : 190000`` is 1,900.00);
    ``DENOMINATION``
        no amount was logged at all, so the notes handled *are* the amount -
        this is how a bill payment is recorded.

    When a note breakdown is available it decides the scale instead of the
    constant: ``raw`` is only divided when the division is what matches the
    notes, so a firmware that logs rupees is never divided as well. With no
    notes to compare against, ``minor_units`` is trusted as configured.
    """
    if raw is None:
        if note_total is None:
            return None, None
        return float(note_total), "DENOMINATION"

    scaled = raw / MINOR_UNIT_DIVISOR if MINOR_UNIT_DIVISOR else raw
    if note_total is not None:
        if abs(raw - note_total) <= AMOUNT_TOLERANCE:
            return float(raw), "REQUEST"
        if abs(scaled - note_total) <= AMOUNT_TOLERANCE:
            return float(scaled), "REQUEST_SCALED"
    if minor_units:
        return float(scaled), "REQUEST_SCALED"
    return float(raw), "REQUEST"


def amount_difference(note_total: Optional[float],
                      amount: Optional[float]) -> Optional[float]:
    """``DENOM_AMOUNT - AMOUNT``, or None when either side is missing."""
    if note_total is None or amount is None:
        return None
    return round(float(note_total) - float(amount), 2)


def amounts_agree(note_total: Optional[float], amount: Optional[float]) -> bool:
    """True when the notes handled are worth exactly the amount recorded."""
    difference = amount_difference(note_total, amount)
    return difference is not None and abs(difference) <= AMOUNT_TOLERANCE


def _function_status(value: Optional[str]) -> Optional[str]:
    """
    Map a bill payment's ``Function Status : True/False`` onto OK / Failed.

    The biller legs report a boolean instead of the ``Status : OK`` the deposit
    legs use, so it is translated into a token :func:`determine_status`
    understands rather than teaching that function about booleans.
    """
    if value is None:
        return None
    token = str(value).strip().lower()
    if token in {"true", "1", "yes", "y"}:
        return "OK"
    if token in {"false", "0", "no", "n"}:
        return "Failed"
    return value


def _split_biller(value: Optional[str]) -> tuple:
    """
    ``Biller Data(ID/DESC) : CEB Only/CEB Only`` -> ``("CEB Only", "CEB Only")``.

    The two halves are usually the same but need not be, so both are kept.
    """
    if not value:
        return None, None
    text = str(value).strip()
    if "/" in text:
        biller_id, _, description = text.partition("/")
        return (biller_id.strip() or None), (description.strip() or text)
    return text or None, text or None


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
    # The terminal ID is a property of the machine and is printed wherever the
    # firmware feels like it - often after the transaction that needs it (the
    # cardless legs print it only in the closing banner), so it is read once for
    # the whole file and used for any record that did not see it in time.
    file_terminal = _file_terminal_id(buffer)

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
        if SESSION_END_RE.search(msg) or SESSION_CLOSE_RE.search(msg):
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

        if DENOMINATE_DONE_RE.search(msg):
            # The mix the ATM *planned*; the dispenser may still pay a different
            # combination, so this is kept only for comparison.
            # A cardless (wallet) withdrawal has no "Trx Started" to open the
            # session, so the planned mix is what opens it implicitly - without
            # this the wallet rows would lose PLANNED_DENOMINATION.
            if session is None:
                session = open_session(implicit=True)
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
            continue

        # ---- fast cash block (same record shape, different anchor) ----------
        if CAPTURE_FAST_CASH and FAST_CASH_START_RE.match(msg):
            if session is None:
                session = open_session(implicit=True)
            stats.fast_cash_records_detected += 1
            record = parse_withdrawal(buffer, index, session, source_file, stats,
                                      ok_re=FAST_CASH_OK_RE,
                                      fail_re=FAST_CASH_FAIL_RE,
                                      txn_type="FAST_CASH",
                                      scan_trailing_fields=True)
            if record:
                records.append(record)
            continue

        # ---- iWallet cash withdrawal (cardless, same record shape) ----------
        # The wallet leg has no card session of its own, so an implicit session
        # is opened for it; the host fields sit after the terminator and the
        # amount is logged in minor units, hence the three flags.
        if CAPTURE_IWALLET and IWALLET_START_RE.match(msg):
            if session is None:
                session = open_session(implicit=True)
            stats.iwallet_records_detected += 1
            record = parse_withdrawal(buffer, index, session, source_file, stats,
                                      ok_re=IWALLET_OK_RE,
                                      fail_re=IWALLET_FAIL_RE,
                                      txn_type="IWALLET_WITHDRAWAL",
                                      scan_trailing_fields=True,
                                      trailing_lines=IWALLET_TRAILING_LINES,
                                      terminator_implies_status=True,
                                      minor_unit_amount=True)
            if record:
                records.append(record)
            continue

    for record in records:
        if not record.get("TERMINAL_ID"):
            record["TERMINAL_ID"] = file_terminal
    return records


def _file_terminal_id(buffer: List[LogLine]) -> Optional[str]:
    """First 'Terminal ID : ...' anywhere in the file, or None."""
    for line in buffer:
        if not line.msg:
            continue
        found = TERMINAL_RE.search(line.msg)
        if found:
            return found.group("tid").strip() or None
    return None


# --------------------------------------------------------------------------- #
# 3. Withdrawal block parsing
# --------------------------------------------------------------------------- #


def parse_withdrawal(buffer: List[LogLine],
                     start: int,
                     session: SessionContext,
                     source_file: str,
                     stats: ParseStats,
                     ok_re: "re.Pattern" = WITHDRAW_OK_RE,
                     fail_re: "re.Pattern" = WITHDRAW_FAIL_RE,
                     txn_type: str = "WITHDRAWAL",
                     scan_trailing_fields: bool = False,
                     trailing_lines: int = 8,
                     terminator_implies_status: bool = False,
                     minor_unit_amount: bool = False) -> Optional[Dict[str, Any]]:

    """
    Parse one withdrawal block into a flat record dict.

    The block runs from the initiation line to its terminator
    ('Cash Withdraw Initiated Completed/Fail', 'Fast Cash Completed',
    'iWallet Cash Withdraw Successful'); dispense and cash-taken outcomes that
    follow inside the same transaction are attached as supplementary fields.

    The anchors differ per transaction type, so they are parameters rather than
    three copies of this function:

    ``ok_re`` / ``fail_re``
        the terminator patterns for this type;
    ``scan_trailing_fields`` / ``trailing_lines``
        read ``Key : Value`` lines printed *after* the terminator (Fast Cash
        prints the account there; the wallet leg prints RESP_CODE / TRACE_NO);
    ``terminator_implies_status``
        the type has no ``Withdraw Status`` field - the terminator suffix is the
        outcome ("... Completed" vs "... Failed");
    ``minor_unit_amount``
        the amount field is logged in minor units (the wallet leg's
        ``Trx Amount : 190000``), to be reconciled against the notes dispensed.
    """
    header = buffer[start]
    stats.withdrawal_records_detected += 1

    fields: Dict[str, str] = {}
    terminator: Optional[str] = None
    aux_no: Optional[str] = None
    aux_seq: Optional[str] = None
    end_index = start
    response_dt: Optional[datetime] = None
    terminator_failed = False

    limit = min(len(buffer), start + WITHDRAW_BLOCK_LOOKAHEAD)
    for index in range(start + 1, limit):
        line = buffer[index]
        msg = line.msg
        if not msg:
            continue

        if fail_re.match(msg):
            terminator = msg
            terminator_failed = True
            end_index = index
            response_dt = line.timestamp
            break
        if ok_re.match(msg):
            terminator = msg
            end_index = index
            response_dt = line.timestamp
            break
        # A new block boundary means this withdrawal was never concluded.
        if (_is_block_start(msg) or TXN_END_RE.search(msg)
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

    # Fast Cash prints "Account Number" and "Trace ID" on the lines AFTER its
    # terminator, so those are picked up here before the outcome scan starts.
    if scan_trailing_fields and terminator:
        for index in range(end_index + 1, min(len(buffer), end_index + trailing_lines)):
            msg = buffer[index].msg
            if not msg:
                continue
            if (_is_block_start(msg) or DISPENSE_CMD_RE.search(msg)
                    or TXN_START_RE.search(msg) or TXN_END_RE.search(msg)
                    or SESSION_END_RE.search(msg) or SESSION_CLOSE_RE.search(msg)):
                break                       # the next operation starts here
            parsed = _field(msg)
            if not parsed:
                break                       # first non key:value line ends the tail
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
    outcome_currency: Optional[str] = None
    in_dispense = False

    for index in range(end_index + 1, min(len(buffer), end_index + OUTCOME_LOOKAHEAD)):
        msg = buffer[index].msg
        if not msg:
            continue
        if _is_block_start(msg) or SESSION_END_RE.search(msg) or TXN_START_RE.search(msg):
            break
        if DISPENSE_CMD_RE.search(msg):
            in_dispense = True
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
        if parsed and parsed[0] == "amount" and dispensed_amount is None and in_dispense:
            # The dispenser prints its amount inside the "Dispense Command
            # Executed" block, i.e. before "Dispense Succeeded" - keying this off
            # the block flag rather than the result is what makes
            # DISPENSED_AMOUNT land for every withdrawal type.
            dispensed_amount = _to_number(parsed[1])
        if parsed and parsed[0] in {"mix", "mix number"} and mix_number is None:
            mix_number = parsed[1]
        if (parsed and parsed[0] in {"currency", "currency id"} and in_dispense
                and outcome_currency is None):
            outcome_currency = parsed[1] or None
        if TXN_END_RE.search(msg):
            break

    # The wallet leg calls it "Trx Amount" (and logs it in minor units), the
    # card legs "Amount", Fast Cash "Requestd Amount". The type's own field is
    # consulted first so a stray amount from a neighbouring block cannot win.
    amount_keys = (("trx amount", "amount", "requestd amount", "requested amount")
                   if minor_unit_amount else
                   ("amount", "trx amount", "requestd amount", "requested amount"))
    raw_amount = next((value for value in (_to_number(fields.get(key))
                                           for key in amount_keys) if value is not None), None)

    denom_amount = denomination_value(denom_counts)
    amount, amount_source = reconcile_amount(raw_amount, denom_amount,
                                             minor_units=minor_unit_amount)
    if amount_source == "REQUEST_SCALED":
        stats.amount_scale_corrections += 1

    account = (fields.get("account") or fields.get("account number") or "").strip() or None
    status_field = fields.get("withdraw status")
    if status_field is None and terminator and (terminator_implies_status
                                                or txn_type == "FAST_CASH"):
        # "Fast Cash Completed OK" / "iWallet Cash Withdraw Successful" - the
        # type has no status field, the terminator suffix is the outcome.
        status_field = "Failed" if terminator_failed else "OK"
    response_code = (fields.get("response") or fields.get("response code")
                     or fields.get("resp_code") or "").strip() or None
    status = determine_status(status_field, terminator, response_code)

    if denom_counts:
        stats.withdrawals_with_denomination += 1
        if not amounts_agree(denom_amount, amount):
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
        "TRANSACTION_TYPE": txn_type,
        "ACCOUNT_NO": account,
        "CARD_NO": session.card_no,
        "AMOUNT": amount,
        "AMOUNT_RAW": raw_amount,
        "AMOUNT_SOURCE": amount_source,
        "REQUESTED_AMOUNT": session.requested_amount,
        "CURRENCY": (fields.get("currency") or fields.get("currency id")
                     or session.currency or outcome_currency),
        "STATUS": status,
        "RESPONSE_CODE": response_code,
        "ACTION_CODE": (fields.get("action code") or fields.get("act_code")
                        or "").strip() or None,
        "TRANSACTION_REF": (aux_no or (fields.get("aux no") or fields.get("aux number")
                                       or "").strip() or None),
        "AUX_SEQ": aux_seq,
        "TRACE_ID": (fields.get("trace id") or fields.get("trace_no")
                     or "").strip() or None,
        "TERMINAL_ID": session.terminal_id or (fields.get("terminal id") or "").strip() or None,
        "CARD_SCHEME": session.card_scheme,
        "FAST_CASH": session.fast_cash,
        # --- wallet-withdrawal specifics (None for card withdrawals) ---------
        "WALLET_ID": (fields.get("wallet id") or "").strip() or None,
        "BANK_CODE": (fields.get("bank code") or "").strip() or None,
        "RET_REF_NO": (fields.get("ret ref no") or fields.get("ret referenece no")
                       or fields.get("ret reference no") or "").strip() or None,
        "FEE": (fields.get("fee") or "").strip() or None,
        "DISPENSE_RESULT": dispense_result,
        "DISPENSED_AMOUNT": dispensed_amount,
        "DENOMINATION": format_denomination(denom_counts),
        "DENOM_BREAKDOWN": denom_counts or None,
        "NOTES_COUNT": int(sum(denom_counts.values())) if denom_counts else None,
        "DENOM_AMOUNT": denom_amount,
        "DENOM_AMOUNT_DIFF": amount_difference(denom_amount, amount),
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

    for col in ("AMOUNT", "AMOUNT_RAW", "REQUESTED_AMOUNT", "DISPENSED_AMOUNT",
                "DENOM_AMOUNT", "DENOM_AMOUNT_DIFF"):
        df[col] = pd.to_numeric(df.get(col), errors="coerce")

    for col in ("ATM_NO", "TRANSACTION_TYPE", "ACCOUNT_NO", "CARD_NO", "TRANSACTION_REF",
                "TRACE_ID", "RESPONSE_CODE", "TERMINAL_ID", "CARD_SCHEME", "TRX_ERROR",
                "SESSION_ID", "SOURCE_FILE", "AUX_SEQ", "ACTION_CODE", "AMOUNT_SOURCE",
                "WALLET_ID", "BANK_CODE", "RET_REF_NO", "FEE"):
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
    # Only a comparison that can actually be made is reported: a row with no
    # amount recorded (an abandoned session) is neither a match nor a mismatch.
    df["DENOM_MATCHES_AMOUNT"] = (denom_amount - amount).abs().le(AMOUNT_TOLERANCE).where(
        denom_amount.notna() & amount.notna(), pd.NA)

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


# --------------------------------------------------------------------------- #
# 7b. Single-file entry point (used by the batched Greenplum loader)
# --------------------------------------------------------------------------- #


def process_single_file(path: str,
                        atm_no: Optional[str] = None,
                        stats: Optional[ParseStats] = None,
                        **dedup_options: Any) -> pd.DataFrame:
    """
    Parse ONE journal file and return its withdrawal-level DataFrame.

    ``atm_no`` defaults to the name of the folder holding the file, matching the
    convention used by :func:`process_all_atms`. Extra keyword arguments are
    forwarded to :func:`deduplicate_attempts`.
    """
    stats = stats or ParseStats()
    atm = str(atm_no or os.path.basename(os.path.dirname(os.path.abspath(path))))

    records = extract_transaction_blocks(read_ejournal_file(path, stats), path, stats)
    for rec in records:
        rec["ATM_NO"] = atm
        rec["SESSION_ID"] = f"{atm}|{rec['SESSION_ID']}"
    stats.files_processed += 1

    kept = deduplicate_attempts(records, stats, **dedup_options)
    df = _build_dataframe(kept)
    df.attrs["stats"] = stats.as_dict()
    df.attrs["unparsed"] = _build_unparsed(records)
    return df


# =========================================================================== #
# 8. CASH DEPOSITS  (added: withdrawal code above is untouched)
# =========================================================================== #
#
# Deposit grammar observed in the journals
# ----------------------------------------
# A cardless deposit is NOT wrapped in Trx Started / #TRANSACTION-START#; it
# runs inside a "Create Session ... Close Session" pair:
#
#     -Create Session-----------------------
#     -Cardless Cash Account Validation Request ----------   <- record anchor
#     ----AUX NO : 2845 :xx:A002301120260910010439-01
#     -----Response Code :000
#     -----Trace ID      :537872
#     -----Action Code   :suc
#     -----Customer Name :U S HASHAN
#     -----Customer Acct :106252785510
#     -----Deposit Thrsld:20000
#     ---Cardless Account Validation Completed
#     ...
#     -Accepting Cash In Succeeded            <- notes actually accepted
#     ----Denom       : 5000
#     ----QTY         : 5
#     ----Denom      : 000100-000003:000500-000002:005000-000005
#     ...
#     -Cardless Cash Deposit Request--------------
#     -----A/C # : 106252**5510
#     -----Amount : 2630000                  <- MINOR UNITS (cents)
#     -----Status : deposit_ok               <- CDM side
#     -----Denom  : 000100-000003:...
#     -----NAR    : Sales and Business Turnover
#     -----Cardless Cash Deposit Status : OK  <- host side
#     ---Cardless Cash Deposit Request Completed
#
# Encoded below:
# * The customer identity (mobile / NIC / account / name) is only available in
#   the validation block, so the session is walked statefully exactly like the
#   withdrawal pass.
# * "Amount" in the deposit request is in cents - every deposit in the sample
#   file is exactly 100x the value of the notes accepted. AMOUNT is the rupee
#   value; AMOUNT_RAW keeps the number as logged.
# * Card-based deposits ("Cash Deposit Request", no validation block) are also
#   captured, flagged DEPOSIT_TYPE = CARD.
# * A validation with no deposit request means the customer walked away, or the
#   cash was rolled back. Those rows are kept with STATUS = NO_DEPOSIT because
#   cash-in-without-credit is exactly what reconciliation needs to see.
#
# Bill payment deposits
# ---------------------
# A cardless bill payment takes cash exactly like a deposit, but credits a
# biller instead of an account, so it has no "Cash Deposit Request" block:
#
#     Entered Mobile No: 0758994275          <- customer keystrokes
#     Entered NIC :941693164V
#     -Accepting Cash In Succeeded           <- notes accepted (= the amount)
#     ----Denom      : 000100-000004:000500-000003
#     -Cardless BillPayment - Biller Verification
#     ----Biller Data(ID/DESC) : CEB Only/CEB Only
#     -----Function Status    : True         <- boolean instead of Status : OK
#     -----Trace ID           : 364042
#     -Cardless Bill Payment Confirm Request <- record anchor
#     ---Cardless Bill Payment Confirm Completed
#     -----Function Status    : True
#     -----Trace ID           : 364027
#     ----Reciept Type : CARDLESS_BILL_PAYMENT
#
# It is emitted as an ordinary deposit row with DEPOSIT_TYPE = BILL_PAYMENT.
# No amount is logged anywhere in the bill payment legs, so the notes accepted
# *are* the amount (AMOUNT_SOURCE = DENOMINATION).
#
# Rejected notes inside one session
# ---------------------------------
# "Accepting Cash In Succeeded" reports the running total of the escrow, not the
# notes added by the last insertion, and it is logged again after every
# insertion. So when the machine refuses notes and the customer re-inserts
# ("Refuse Items Found" / "Items Taken" / insert again), the breakdown appears
# two or three times with the last one being the total that is credited.
# Summing them would multiply the deposit, so the snapshots are kept in order
# and the FINAL one (or the one that matches the host amount) is the record's
# breakdown - see :func:`select_final_denominations`.

#: Deposit amounts are logged in minor units (cents). Set to 1 if a firmware
#: version ever logs rupees.
DEPOSIT_AMOUNT_DIVISOR = 100.0

#: Lines scanned after a deposit / validation anchor for its own fields.
DEPOSIT_BLOCK_LOOKAHEAD = 60

DEPOSIT_FINAL_COLUMNS = [
    "ATM_NO",
    "TRANSACTION_DATETIME",
    "DATE",
    "TIME",
    "DEPOSIT_TYPE",
    "ACCOUNT_NO",
    "ACCOUNT_MASKED",
    "ACCOUNT_TYPE",
    "CUSTOMER_NAME",
    "MOBILE_NO",
    "NIC_NO",
    "CARD_NO",
    "AMOUNT",
    "AMOUNT_RAW",
    "AMOUNT_SOURCE",
    "CURRENCY",
    "STATUS",
    "DEPOSIT_STATUS",
    "RESPONSE_CODE",
    "ACTION_CODE",
    "TRANSACTION_REF",
    "TRACE_ID",
    "TERMINAL_ID",
    "NARRATION",
    "DEPOSIT_THRESHOLD",
    "BILLER_ID",
    "BILLER_NAME",
    "BILL_REFERENCE_NO",
    "DENOMINATION",
    "NOTES_COUNT",
    "DENOM_AMOUNT",
    "DENOM_AMOUNT_DIFF",
    "CASH_IN_RESULT",
    "CASH_IN_ATTEMPTS",
    "NOTES_REFUSED",
    "REFUSE_REASON",
    "CASH_IN_ROLLBACK",
    "RECEIPT_TYPE",
    "TRX_ERROR",
    "VALIDATION_DATETIME",
    "VALIDATION_RESPONSE_CODE",
    "VALIDATION_TRACE_ID",
    "VALIDATION_ACTION_CODE",
    "VALIDATION_REF",
    "DEPOSIT_SEQ",
    "SESSION_ID",
    "SOURCE_FILE",
    "SOURCE_LINE",
]

# --- deposit-specific patterns --------------------------------------------- #

CREATE_SESSION_RE = re.compile(r"^Create\s+Session$", re.I)
CLOSE_SESSION_RE = re.compile(r"^Close\s+Session\b|#SESSION-END#", re.I)

VALIDATION_START_RE = re.compile(r"^Cardless\s+Cash\s+Account\s+Validation\s+Request", re.I)
VALIDATION_DONE_RE = re.compile(r"Cardless\s+Account\s+Validation\s+(Completed|Fail)", re.I)

DEPOSIT_START_RE = re.compile(r"^(Cardless\s+)?Cash\s+Deposit\s+Request$", re.I)
DEPOSIT_OK_RE = re.compile(r"^(Cardless\s+)?Cash\s+Deposit\s+Request\s+Completed", re.I)
DEPOSIT_FAIL_RE = re.compile(r"^(Cardless\s+)?Cash\s+Deposit\s+Request\s+(Fail|Failed)", re.I)

ENTERED_MOBILE_RE = re.compile(r"Entered\s+Mobile\s*No\s*:\s*(?P<val>[0-9+]{6,})", re.I)
ENTERED_NIC_RE = re.compile(r"Entered\s+NIC\s*:\s*(?P<val>[A-Za-z0-9]{5,})", re.I)
ENTERED_ACCOUNT_RE = re.compile(r"Entered\s+Account\s*No\s*:\s*(?P<val>[0-9]{4,})", re.I)

CASH_IN_OK_RE = re.compile(r"^Accepting\s+Cash\s+In\s+Succeeded", re.I)
CASH_IN_FAIL_RE = re.compile(r"^Accepting\s+Cash\s+In\s+(Failed|Fail)", re.I)
CASH_IN_ROLLBACK_RE = re.compile(r"CASH\s+IN\s+ROLLBACK", re.I)
REFUSE_RE = re.compile(r"Refuse\s+Items\s+Found", re.I)
REASON_RE = re.compile(r"^Reason\s*:\s*(?P<val>.+)$", re.I)
RECEIPT_TYPE_RE = re.compile(r"^Rec(?:ei|ie)pt\s*Type\s*:\s*(?P<val>\S+)", re.I)

# --- bill payment (cash accepted, biller credited) ------------------------- #
#: "Cardless BillPayment - Biller Verification" (the firmware writes
#: "BillPayment" without the space here and with it everywhere else).
BILL_VERIFY_RE = re.compile(
    r"^(Cardless\s+)?Bill\s*Payment\s*-\s*Biller\s+Verification", re.I)
#: "Cardless Bill Payment Confirm Request" - the anchor the row is built on.
BILL_CONFIRM_RE = re.compile(
    r"^(Cardless\s+)?Bill\s*Payment\s+Confirm\s+Request", re.I)
BILL_CONFIRM_OK_RE = re.compile(
    r"^(Cardless\s+)?Bill\s*Payment\s+Confirm\s+Completed", re.I)
BILL_CONFIRM_FAIL_RE = re.compile(
    r"^(Cardless\s+)?Bill\s*Payment\s+Confirm\s+(Fail|Failed|Rejected|Declined)", re.I)
#: ``Biller Data(ID/DESC) : CEB Only/CEB Only`` - the brackets keep this line
#: out of FIELD_RE, so it has its own pattern.
BILLER_DATA_RE = re.compile(
    r"^Biller\s*Data\s*(?:\([^)]*\))?\s*:\s*(?P<val>.+)$", re.I)

#: Lines scanned after a bill payment terminator for its host fields
#: (Function Status / Trace ID / Ret Referenece No are printed after it).
BILL_TRAILING_LINES = 10

#: Where a cash-in note breakdown stops (the deposit block layout differs from
#: the dispenser's, so the withdrawal stop-list would run past the end).
#:
#: The next cash insertion is a boundary too: a session where notes are refused
#: and re-inserted logs several "Accepting Cash In" blocks, and without these
#: anchors one snapshot's scan would run into the next one and add the two
#: together - the very double counting the snapshot logic exists to avoid.
DEPOSIT_DENOM_STOP_RE = re.compile(
    r"Getting\s+Depositor\s+Status|Open\s+Shutter|Close\s+Shutter|CASH\s+UNIT\s+INFO|"
    r"Ending\s+Cash\s+In|Cash\s+Deposit\s+Request|#SESSION-|Trx\s+End|Trx\s+Started|"
    r"Create\s+Session|Close\s+Session|Accepting\s+Cash\s+In|Refuse\s+Items|"
    r"Items\s+Inserted|Items\s+Taken|Bill\s*Payment",
    re.I,
)


@dataclass
class DepositParseStats:
    """Audit counters for the deposit pass (kept separate from ParseStats)."""

    atm_folders_processed: int = 0
    atm_folders_without_files: int = 0
    files_processed: int = 0
    files_failed: int = 0
    lines_read: int = 0
    lines_unrecognised: int = 0
    deposit_sessions: int = 0
    validations_seen: int = 0
    deposit_records_detected: int = 0
    bill_payment_records_detected: int = 0
    cash_in_events: int = 0
    superseded_cash_in_snapshots: int = 0
    successful_deposits: int = 0
    failed_deposits: int = 0
    unknown_status_deposits: int = 0
    abandoned_after_validation: int = 0
    cash_in_without_deposit: int = 0
    deposits_with_denomination: int = 0
    denomination_amount_mismatches: int = 0
    amount_scale_corrections: int = 0
    records_unparsed: int = 0

    def as_dict(self) -> Dict[str, int]:
        return asdict(self)


@dataclass
class DepositContext:
    """State carried across one deposit session (Create Session .. Close Session)."""

    session_id: str
    opened_at: Optional[datetime] = None
    mobile_no: Optional[str] = None
    nic_no: Optional[str] = None
    entered_account: Optional[str] = None
    card_no: Optional[str] = None
    terminal_id: Optional[str] = None
    currency: Optional[str] = None
    validation: Dict[str, Any] = field(default_factory=dict)
    bill_payment: Dict[str, Any] = field(default_factory=dict)
    cash_in: Dict[str, Any] = field(default_factory=dict)
    deposit_seq: int = 0
    implicit: bool = False


def extract_deposit_denominations(buffer: List[LogLine],
                                  start: int,
                                  end: int) -> Dict[int, int]:
    """
    Note breakdown for a deposit.

    The CDM logs the same breakdown twice - once as per-type triplets
    (``Denom : 5000`` / ``QTY : 5``) and once as a compact string
    (``Denom : 005000-000005:000100-000003``). Counting both would double every
    deposit, so the compact line wins when present and the triplets are the
    fallback.
    """
    compact: Dict[int, int] = {}
    triplets: Dict[int, int] = {}
    pending_value: Optional[int] = None

    for index in range(start + 1, min(end, len(buffer))):
        msg = buffer[index].msg
        if not msg:
            continue
        if DEPOSIT_DENOM_STOP_RE.search(msg):
            break

        if DENOM_LINE_RE.match(msg):
            pairs = DENOM_COMPACT_RE.findall(msg)
            if pairs:
                for value_token, num_token in pairs:
                    value, num = int(value_token), int(num_token)
                    if value > 0 and num > 0:
                        compact[value] = compact.get(value, 0) + num
                pending_value = None
                continue

        triplet = DENOM_QTY_RE.match(msg)
        if triplet:
            key = triplet.group("key").lower().replace(" ", "")
            value = _to_number(triplet.group("val"))
            if key == "denom" and value:
                pending_value = int(value)
            elif key == "qty" and pending_value and value:
                triplets[pending_value] = triplets.get(pending_value, 0) + int(value)
                pending_value = None

    return compact or triplets


def _deposit_amount(raw: Optional[float]) -> Optional[float]:
    """Convert a logged deposit amount (minor units) to the rupee value."""
    if raw is None:
        return None
    return raw / DEPOSIT_AMOUNT_DIVISOR if DEPOSIT_AMOUNT_DIVISOR else raw


def select_final_denominations(cash_in: Dict[str, Any],
                               candidate_amounts: Optional[Iterable[Optional[float]]] = None
                               ) -> tuple:
    """
    The notes that belong to THIS deposit - the *final* accepted breakdown.

    Returns ``(breakdown, attempts)``.

    Why the last one and not the sum: "Accepting Cash In Succeeded" prints the
    running total held in the escrow, and it is printed again after every
    insertion. In a rejection session (notes refused, customer re-inserts) the
    same breakdown therefore appears two or three times, each one a superset of
    the one before:

        attempt 1   1000x2 + 5000x1   ->  7,000   (one note refused)
        attempt 2   1000x2 + 5000x1   ->  7,000   (re-inserted, refused again)
        attempt 3   1000x2 + 5000x2   -> 12,000   <- credited, "Amount : 1200000"

    Adding them up gives 26,000 for a 12,000 deposit, which is the bug this
    function exists to prevent: only the last snapshot is the deposit.

    When the host amount is known it decides instead of the order: the newest
    snapshot whose value equals the amount wins (``candidate_amounts`` carries
    both readings of the logged amount - as logged and divided by 100 - because
    the scale is not yet settled at this point). That keeps the row correct even
    if a firmware version prints a further status line after the credited one.
    """
    snapshots = [dict(snapshot) for snapshot in (cash_in.get("snapshots") or []) if snapshot]
    if not snapshots:
        return dict(cash_in.get("denominations") or {}), 0

    chosen = snapshots[-1]
    wanted = [a for a in (candidate_amounts or []) if a is not None]
    if wanted and not any(amounts_agree(denomination_value(chosen), a) for a in wanted):
        for snapshot in reversed(snapshots):
            if any(amounts_agree(denomination_value(snapshot), a) for a in wanted):
                chosen = snapshot
                break
    return chosen, len(snapshots)


def parse_validation(buffer: List[LogLine], start: int) -> Dict[str, Any]:
    """
    Read a 'Cardless Cash Account Validation Request' block.

    This is where the customer is identified - account number, name and the
    deposit threshold the host allows - so it is the anchor the deposit record
    is built on.
    """
    fields: Dict[str, str] = {}
    aux_no: Optional[str] = None

    limit = min(len(buffer), start + DEPOSIT_BLOCK_LOOKAHEAD)
    for index in range(start + 1, limit):
        msg = buffer[index].msg
        if not msg:
            continue
        if VALIDATION_DONE_RE.search(msg):
            break
        if (DEPOSIT_START_RE.match(msg) or CLOSE_SESSION_RE.search(msg)
                or CREATE_SESSION_RE.match(msg)):
            break
        aux = AUX_NO_RE.search(msg)
        if aux:
            aux_no = aux.group("aux")
            continue
        parsed = _field(msg)
        if parsed and (parsed[0] not in fields or not fields[parsed[0]]):
            fields[parsed[0]] = parsed[1]

    return {
        "datetime": buffer[start].timestamp,
        "response_code": (fields.get("response code") or fields.get("response") or "").strip() or None,
        "trace_id": (fields.get("trace id") or "").strip() or None,
        "action_code": (fields.get("action code") or "").strip() or None,
        "customer_name": (fields.get("customer name") or "").strip() or None,
        "customer_account": (fields.get("customer acct") or fields.get("customer account") or "").strip() or None,
        "deposit_threshold": _to_number(fields.get("deposit thrsld") or fields.get("deposit threshold")),
        "ref": aux_no,
    }


def parse_deposit(buffer: List[LogLine],
                  start: int,
                  session: DepositContext,
                  source_file: str,
                  stats: DepositParseStats) -> Dict[str, Any]:
    """
    Parse one 'Cash Deposit Request' / 'Cardless Cash Deposit Request' block
    into a flat record, merged with the session context that precedes it.
    """
    header = buffer[start]
    stats.deposit_records_detected += 1
    session.deposit_seq += 1

    cardless = bool(re.match(r"^Cardless", header.msg, re.I))
    fields: Dict[str, str] = {}
    terminator: Optional[str] = None
    aux_no: Optional[str] = None
    aux_seq: Optional[str] = None
    denom_counts: Dict[int, int] = {}
    end_index = start

    limit = min(len(buffer), start + DEPOSIT_BLOCK_LOOKAHEAD)
    for index in range(start + 1, limit):
        line = buffer[index]
        msg = line.msg
        if not msg:
            continue

        if DEPOSIT_OK_RE.match(msg) or DEPOSIT_FAIL_RE.match(msg):
            terminator = msg
            end_index = index
            break
        if (DEPOSIT_START_RE.match(msg) or CLOSE_SESSION_RE.search(msg)
                or CREATE_SESSION_RE.match(msg) or SESSION_START_RE.search(msg)):
            end_index = index - 1
            break

        aux = AUX_NO_RE.search(msg)
        if aux:
            aux_seq, aux_no = aux.group("seq"), aux.group("aux")
            continue

        if DENOM_LINE_RE.match(msg):
            for value_token, num_token in DENOM_COMPACT_RE.findall(msg):
                value, num = int(value_token), int(num_token)
                if value > 0 and num > 0:
                    denom_counts[value] = denom_counts.get(value, 0) + num
            continue

        parsed = _field(msg)
        if parsed:
            key, value = parsed
            if key not in fields or (not fields[key] and value):
                fields[key] = value
        end_index = index

    # Card deposits carry no Denom line - fall back to what the CDM accepted.
    # Only the FINAL accepted breakdown belongs to this deposit; see
    # select_final_denominations for why the snapshots are not summed.
    raw_amount_hint = _to_number(fields.get("amount"))
    cash_in_attempts = 0
    if not denom_counts:
        denom_counts, cash_in_attempts = select_final_denominations(
            session.cash_in,
            candidate_amounts=(raw_amount_hint, _deposit_amount(raw_amount_hint)))
        if cash_in_attempts > 1:
            stats.superseded_cash_in_snapshots += cash_in_attempts - 1
            logger.debug("%s: %d cash-in snapshot(s) superseded, keeping %s",
                         session.session_id, cash_in_attempts - 1,
                         format_denomination(denom_counts))

    # Receipt type / Trx Error are printed just after the block closes.
    receipt_type: Optional[str] = None
    trx_error: Optional[str] = None
    for index in range(end_index + 1, min(len(buffer), end_index + 30)):
        msg = buffer[index].msg
        if not msg:
            continue
        if (DEPOSIT_START_RE.match(msg) or CLOSE_SESSION_RE.search(msg)
                or CREATE_SESSION_RE.match(msg)):
            break
        receipt = RECEIPT_TYPE_RE.match(msg)
        if receipt and receipt_type is None:
            receipt_type = receipt.group("val")
        error = TRX_ERROR_RE.search(msg)
        if error and trx_error is None:
            trx_error = error.group("code")

    host_status = (fields.get("cardless cash deposit status")
                   or fields.get("cash deposit status")
                   or fields.get("deposit status"))
    response_code = (fields.get("response") or fields.get("response code") or "").strip() or None
    status = determine_status(host_status, terminator, response_code)

    amount_raw = raw_amount_hint
    denom_amount = denomination_value(denom_counts)
    # The deposit legs log cents, but the scale is confirmed against the notes
    # rather than assumed, and a disagreement is reported instead of patched.
    amount, amount_source = reconcile_amount(amount_raw, denom_amount, minor_units=True)
    if amount_source == "REQUEST_SCALED":
        stats.amount_scale_corrections += 1

    if denom_counts:
        stats.deposits_with_denomination += 1
        if not amounts_agree(denom_amount, amount):
            stats.denomination_amount_mismatches += 1
            logger.warning("%s: deposit amount %s does not match the notes accepted %s (%s)",
                           session.session_id, amount, denom_amount,
                           format_denomination(denom_counts))

    if status == "SUCCESS":
        stats.successful_deposits += 1
    elif status == "FAILED":
        stats.failed_deposits += 1
    else:
        stats.unknown_status_deposits += 1

    validation = session.validation or {}
    masked = (fields.get("a/c #") or fields.get("a/c#") or fields.get("account") or "").strip() or None

    record = {
        "ATM_NO": None,                       # filled in by the folder wrapper
        "TRANSACTION_DATETIME": header.timestamp,
        "DEPOSIT_TYPE": "CARDLESS" if cardless else "CARD",
        "ACCOUNT_NO": validation.get("customer_account") or session.entered_account,
        "ACCOUNT_MASKED": masked,
        "ACCOUNT_TYPE": (fields.get("a/c type") or "").strip() or None,
        "CUSTOMER_NAME": validation.get("customer_name"),
        "MOBILE_NO": session.mobile_no,
        "NIC_NO": session.nic_no,
        "CARD_NO": session.card_no,
        "AMOUNT": amount,
        "AMOUNT_RAW": amount_raw,
        "AMOUNT_SOURCE": amount_source,
        "CURRENCY": (fields.get("currency id") or fields.get("currency")
                     or session.cash_in.get("currency") or session.currency),
        "STATUS": status,
        "DEPOSIT_STATUS": (fields.get("status") or "").strip() or None,
        "RESPONSE_CODE": response_code,
        "ACTION_CODE": (fields.get("action code") or "").strip() or None,
        "TRANSACTION_REF": aux_no,
        "AUX_SEQ": aux_seq,
        "TRACE_ID": (fields.get("trace id") or "").strip() or None,
        "TERMINAL_ID": session.terminal_id,
        "NARRATION": (fields.get("nar") or fields.get("narration") or "").strip() or None,
        "DEPOSIT_THRESHOLD": validation.get("deposit_threshold"),
        "BILLER_ID": None,
        "BILLER_NAME": None,
        "BILL_REFERENCE_NO": None,
        "DENOMINATION": format_denomination(denom_counts),
        "DENOM_BREAKDOWN": denom_counts or None,
        "NOTES_COUNT": int(sum(denom_counts.values())) if denom_counts else None,
        "DENOM_AMOUNT": denom_amount,
        "DENOM_AMOUNT_DIFF": amount_difference(denom_amount, amount),
        "CASH_IN_RESULT": session.cash_in.get("result"),
        "CASH_IN_ATTEMPTS": cash_in_attempts or session.cash_in.get("attempts") or 0,
        "NOTES_REFUSED": session.cash_in.get("refused"),
        "REFUSE_REASON": session.cash_in.get("refuse_reason"),
        "CASH_IN_ROLLBACK": session.cash_in.get("rollback", False),
        "RECEIPT_TYPE": receipt_type,
        "TRX_ERROR": trx_error,
        "VALIDATION_DATETIME": validation.get("datetime"),
        "VALIDATION_RESPONSE_CODE": validation.get("response_code"),
        "VALIDATION_TRACE_ID": validation.get("trace_id"),
        "VALIDATION_ACTION_CODE": validation.get("action_code"),
        "VALIDATION_REF": validation.get("ref"),
        "DEPOSIT_SEQ": session.deposit_seq,
        "SESSION_ID": session.session_id,
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


def parse_bill_verification(buffer: List[LogLine], start: int) -> Dict[str, Any]:
    """
    Read a 'Cardless BillPayment - Biller Verification' block.

    This is where the biller is identified, so it plays the same role for a bill
    payment that :func:`parse_validation` plays for a cardless deposit: it is
    held on the session and merged into the record the confirm request produces.
    """
    biller_id: Optional[str] = None
    biller_name: Optional[str] = None
    fields: Dict[str, str] = {}
    aux_no: Optional[str] = None

    limit = min(len(buffer), start + DEPOSIT_BLOCK_LOOKAHEAD)
    for index in range(start + 1, limit):
        msg = buffer[index].msg
        if not msg:
            continue
        if (BILL_CONFIRM_RE.match(msg) or DEPOSIT_START_RE.match(msg)
                or CLOSE_SESSION_RE.search(msg) or CREATE_SESSION_RE.match(msg)
                or CASH_IN_OK_RE.match(msg)):
            break

        biller = BILLER_DATA_RE.match(msg)
        if biller:
            biller_id, biller_name = _split_biller(biller.group("val"))
            continue

        aux = AUX_NO_RE.search(msg)
        if aux:
            aux_no = aux.group("aux")
            continue

        parsed = _field(msg)
        if parsed and (parsed[0] not in fields or not fields[parsed[0]]):
            fields[parsed[0]] = parsed[1]

    return {
        "datetime": buffer[start].timestamp,
        "biller_id": biller_id,
        "biller_name": biller_name,
        "function_status": (fields.get("function status") or "").strip() or None,
        "trace_id": (fields.get("trace id") or "").strip() or None,
        "ref": aux_no,
        "line": buffer[start].lineno,
    }


def parse_bill_payment(buffer: List[LogLine],
                       start: int,
                       session: DepositContext,
                       source_file: str,
                       stats: DepositParseStats) -> Dict[str, Any]:
    """
    Parse one 'Bill Payment Confirm Request' into a deposit-shaped record.

    A bill payment is a cash deposit whose credit goes to a biller, so it is
    emitted into the deposit DataFrame with DEPOSIT_TYPE = BILL_PAYMENT and the
    same columns every other deposit row uses.

    Two things differ from a deposit request and are handled here:

    * the legs log **no amount at all**, so the notes the CDM accepted are the
      amount (AMOUNT_SOURCE = DENOMINATION);
    * the outcome is a boolean ``Function Status : True`` instead of
      ``Status : OK``, and it is printed *after* the terminator, so the trailing
      lines are scanned as well.
    """
    header = buffer[start]
    stats.deposit_records_detected += 1
    stats.bill_payment_records_detected += 1
    session.deposit_seq += 1

    fields: Dict[str, str] = {}
    terminator: Optional[str] = None
    terminator_failed = False
    aux_no: Optional[str] = None
    aux_seq: Optional[str] = None
    end_index = start

    limit = min(len(buffer), start + DEPOSIT_BLOCK_LOOKAHEAD)
    for index in range(start + 1, limit):
        msg = buffer[index].msg
        if not msg:
            continue
        if BILL_CONFIRM_FAIL_RE.match(msg):
            terminator, terminator_failed, end_index = msg, True, index
            break
        if BILL_CONFIRM_OK_RE.match(msg):
            terminator, end_index = msg, index
            break
        if (BILL_CONFIRM_RE.match(msg) or BILL_VERIFY_RE.match(msg)
                or DEPOSIT_START_RE.match(msg) or CLOSE_SESSION_RE.search(msg)
                or CREATE_SESSION_RE.match(msg) or SESSION_START_RE.search(msg)):
            end_index = index - 1
            break

        aux = AUX_NO_RE.search(msg)
        if aux:
            aux_seq, aux_no = aux.group("seq"), aux.group("aux")
            continue

        parsed = _field(msg)
        if parsed and (parsed[0] not in fields or not fields[parsed[0]]):
            fields[parsed[0]] = parsed[1]
        end_index = index

    # Function Status / Trace ID / Ret Referenece No follow the terminator.
    receipt_type: Optional[str] = None
    trx_error: Optional[str] = None
    for index in range(end_index + 1,
                       min(len(buffer), end_index + 1 + BILL_TRAILING_LINES)):
        msg = buffer[index].msg
        if not msg:
            continue
        if (BILL_CONFIRM_RE.match(msg) or BILL_VERIFY_RE.match(msg)
                or DEPOSIT_START_RE.match(msg) or CLOSE_SESSION_RE.search(msg)
                or CREATE_SESSION_RE.match(msg)):
            break
        receipt = RECEIPT_TYPE_RE.match(msg)
        if receipt and receipt_type is None:
            receipt_type = receipt.group("val")
            continue
        error = TRX_ERROR_RE.search(msg)
        if error and trx_error is None:
            trx_error = error.group("code")
        aux = AUX_NO_RE.search(msg)
        if aux:
            aux_seq = aux_seq or aux.group("seq")
            aux_no = aux_no or aux.group("aux")
            continue
        parsed = _field(msg)
        if parsed and (parsed[0] not in fields or not fields[parsed[0]]):
            fields[parsed[0]] = parsed[1]

    verification = session.bill_payment or {}

    # The notes accepted are the amount - and only the final accepted
    # breakdown, exactly as for a rejected cash deposit.
    denom_counts, cash_in_attempts = select_final_denominations(session.cash_in)
    if cash_in_attempts > 1:
        stats.superseded_cash_in_snapshots += cash_in_attempts - 1
    denom_amount = denomination_value(denom_counts)
    amount, amount_source = reconcile_amount(None, denom_amount)

    status_field = _function_status(fields.get("function status")
                                    or verification.get("function_status"))
    if status_field is None and terminator:
        status_field = "Failed" if terminator_failed else "OK"
    response_code = (fields.get("response") or fields.get("response code") or "").strip() or None
    status = determine_status(status_field, terminator, response_code)

    if denom_counts:
        stats.deposits_with_denomination += 1
    if status == "SUCCESS":
        stats.successful_deposits += 1
    elif status == "FAILED":
        stats.failed_deposits += 1
    else:
        stats.unknown_status_deposits += 1

    record = {
        "ATM_NO": None,                       # filled in by the folder wrapper
        "TRANSACTION_DATETIME": header.timestamp,
        "DEPOSIT_TYPE": "BILL_PAYMENT",
        "ACCOUNT_NO": session.entered_account,
        "ACCOUNT_MASKED": None,
        "ACCOUNT_TYPE": None,
        "CUSTOMER_NAME": (session.validation or {}).get("customer_name"),
        "MOBILE_NO": session.mobile_no,
        "NIC_NO": session.nic_no,
        "CARD_NO": session.card_no,
        "AMOUNT": amount,
        "AMOUNT_RAW": None,                   # the legs log no amount at all
        "AMOUNT_SOURCE": amount_source,
        "CURRENCY": session.cash_in.get("currency") or session.currency,
        "STATUS": status,
        "DEPOSIT_STATUS": (fields.get("function status")
                           or verification.get("function_status") or "").strip() or None,
        "RESPONSE_CODE": response_code,
        "ACTION_CODE": (fields.get("action code") or "").strip() or None,
        "TRANSACTION_REF": aux_no or verification.get("ref"),
        "AUX_SEQ": aux_seq,
        "TRACE_ID": (fields.get("trace id") or verification.get("trace_id") or "").strip() or None,
        "TERMINAL_ID": session.terminal_id,
        "NARRATION": verification.get("biller_name"),
        "DEPOSIT_THRESHOLD": (session.validation or {}).get("deposit_threshold"),
        "BILLER_ID": verification.get("biller_id"),
        "BILLER_NAME": verification.get("biller_name"),
        "BILL_REFERENCE_NO": (fields.get("ret referenece no") or fields.get("ret reference no")
                              or fields.get("reference no") or "").strip() or None,
        "DENOMINATION": format_denomination(denom_counts),
        "DENOM_BREAKDOWN": denom_counts or None,
        "NOTES_COUNT": int(sum(denom_counts.values())) if denom_counts else None,
        "DENOM_AMOUNT": denom_amount,
        "DENOM_AMOUNT_DIFF": amount_difference(denom_amount, amount),
        "CASH_IN_RESULT": session.cash_in.get("result"),
        "CASH_IN_ATTEMPTS": cash_in_attempts,
        "NOTES_REFUSED": session.cash_in.get("refused"),
        "REFUSE_REASON": session.cash_in.get("refuse_reason"),
        "CASH_IN_ROLLBACK": session.cash_in.get("rollback", False),
        "RECEIPT_TYPE": receipt_type,
        "TRX_ERROR": trx_error,
        "VALIDATION_DATETIME": verification.get("datetime"),
        "VALIDATION_RESPONSE_CODE": None,
        "VALIDATION_TRACE_ID": verification.get("trace_id"),
        "VALIDATION_ACTION_CODE": _function_status(verification.get("function_status")),
        "VALIDATION_REF": verification.get("ref"),
        "DEPOSIT_SEQ": session.deposit_seq,
        "SESSION_ID": session.session_id,
        "SOURCE_FILE": os.path.basename(source_file),
        "SOURCE_PATH": source_file,
        "SOURCE_LINE": header.lineno,
        # A bill payment with no cash accepted is not a parse we trust: the
        # notes are the amount, so without them there is nothing to report.
        "PARSE_CONFIDENT": bool(denom_counts and status != "UNKNOWN"),
    }

    if not record["PARSE_CONFIDENT"]:
        stats.records_unparsed += 1
        record["RAW_BLOCK"] = " | ".join(
            b.msg for b in buffer[start:end_index + 1] if b.msg
        )[:2000]

    return record


def _abandoned_record(session: DepositContext,
                      source_file: str,
                      stats: DepositParseStats) -> Optional[Dict[str, Any]]:
    """
    Build a row for a session that took cash, or validated an account, but never
    reached a request that credited it - the customer cancelled, the host
    declined, or the cash was rolled back.

    A session that accepted notes is reported even with no validation block: a
    bill payment has no validation at all, and cash-in-without-credit is exactly
    what reconciliation has to see.
    """
    validation = session.validation or {}
    bill = session.bill_payment or {}
    cash_in = session.cash_in or {}
    denom_counts, attempts = select_final_denominations(cash_in)
    if not (validation or bill or denom_counts):
        return None

    stats.abandoned_after_validation += 1
    if denom_counts:
        stats.cash_in_without_deposit += 1

    denom_amount = denomination_value(denom_counts)
    return {
        "ATM_NO": None,
        "TRANSACTION_DATETIME": (validation.get("datetime") or bill.get("datetime")
                                 or cash_in.get("datetime") or session.opened_at),
        "DEPOSIT_TYPE": "BILL_PAYMENT" if bill else "CARDLESS",
        "ACCOUNT_NO": validation.get("customer_account") or session.entered_account,
        "ACCOUNT_MASKED": None,
        "ACCOUNT_TYPE": None,
        "CUSTOMER_NAME": validation.get("customer_name"),
        "MOBILE_NO": session.mobile_no,
        "NIC_NO": session.nic_no,
        "CARD_NO": session.card_no,
        "AMOUNT": None,
        "AMOUNT_RAW": None,
        "AMOUNT_SOURCE": None,
        "CURRENCY": cash_in.get("currency") or session.currency,
        "STATUS": "NO_DEPOSIT",
        "DEPOSIT_STATUS": None,
        "RESPONSE_CODE": None,
        "ACTION_CODE": None,
        "TRANSACTION_REF": None,
        "AUX_SEQ": None,
        "TRACE_ID": None,
        "TERMINAL_ID": session.terminal_id,
        "NARRATION": bill.get("biller_name"),
        "DEPOSIT_THRESHOLD": validation.get("deposit_threshold"),
        "BILLER_ID": bill.get("biller_id"),
        "BILLER_NAME": bill.get("biller_name"),
        "BILL_REFERENCE_NO": None,
        "DENOMINATION": format_denomination(denom_counts),
        "DENOM_BREAKDOWN": denom_counts or None,
        "NOTES_COUNT": int(sum(denom_counts.values())) if denom_counts else None,
        "DENOM_AMOUNT": denom_amount,
        "DENOM_AMOUNT_DIFF": None,            # no amount was ever recorded
        "CASH_IN_RESULT": cash_in.get("result"),
        "CASH_IN_ATTEMPTS": attempts,
        "NOTES_REFUSED": cash_in.get("refused"),
        "REFUSE_REASON": cash_in.get("refuse_reason"),
        "CASH_IN_ROLLBACK": cash_in.get("rollback", False),
        "RECEIPT_TYPE": None,
        "TRX_ERROR": None,
        "VALIDATION_DATETIME": validation.get("datetime") or bill.get("datetime"),
        "VALIDATION_RESPONSE_CODE": validation.get("response_code"),
        "VALIDATION_TRACE_ID": validation.get("trace_id") or bill.get("trace_id"),
        "VALIDATION_ACTION_CODE": (validation.get("action_code")
                                   or _function_status(bill.get("function_status"))),
        "VALIDATION_REF": validation.get("ref") or bill.get("ref"),
        "DEPOSIT_SEQ": 0,
        "SESSION_ID": session.session_id,
        "SOURCE_FILE": os.path.basename(source_file),
        "SOURCE_PATH": source_file,
        "SOURCE_LINE": None,
        "PARSE_CONFIDENT": True,
    }


def extract_deposit_blocks(lines: Iterable[LogLine],
                           source_file: str,
                           stats: DepositParseStats,
                           include_abandoned: bool = True) -> List[Dict[str, Any]]:
    """
    Walk a file once and emit one raw record per deposit request.

    Mirrors :func:`extract_transaction_blocks` but tracks the cardless session
    (Create Session .. Close Session) instead of the card session, because the
    customer identity and the accepted-notes breakdown both live outside the
    deposit block itself.
    """
    buffer: List[LogLine] = list(lines)
    records: List[Dict[str, Any]] = []

    session: Optional[DepositContext] = None
    session_counter = 0
    base = os.path.basename(source_file)
    file_terminal = _file_terminal_id(buffer)        # see the withdrawal pass

    # Mobile / NIC / account are keyed in BEFORE "Create Session" opens the
    # session, so they are held here and handed to the session when it opens.
    pending: Dict[str, Optional[str]] = {"mobile": None, "nic": None, "account": None}

    def open_session(line: Optional[LogLine] = None, implicit: bool = False) -> DepositContext:
        nonlocal session_counter
        session_counter += 1
        stats.deposit_sessions += 1
        return DepositContext(
            session_id=f"{base}#D{session_counter:05d}",
            opened_at=line.timestamp if line else None,
            mobile_no=pending["mobile"],
            nic_no=pending["nic"],
            entered_account=pending["account"],
            implicit=implicit,
        )

    def remember(kind: str, value: str, line: LogLine) -> None:
        """Record a keystroke on the open session and keep it for the next one."""
        nonlocal session
        pending[kind] = value
        if session is None:
            session = open_session(line, implicit=True)
        if kind == "mobile":
            session.mobile_no = value
        elif kind == "nic":
            session.nic_no = value
        else:
            session.entered_account = value

    def close_session() -> None:
        nonlocal session
        if session is not None and session.deposit_seq == 0 and include_abandoned:
            record = _abandoned_record(session, source_file, stats)
            if record:
                records.append(record)
        session = None
        pending.update({"mobile": None, "nic": None, "account": None})

    for index, line in enumerate(buffer):
        msg = line.msg
        if not msg:
            continue

        # ---- session boundaries -------------------------------------------
        if CREATE_SESSION_RE.match(msg) or SESSION_START_RE.search(msg):
            # The keystrokes captured just before this line belong to the
            # session that is opening now, so carry them across the boundary.
            carried = dict(pending)
            close_session()
            pending.update(carried)
            session = open_session(line)
            continue
        if CLOSE_SESSION_RE.search(msg) or SESSION_END_RE.search(msg):
            close_session()
            continue

        # ---- customer keystrokes before validation -------------------------
        mobile = ENTERED_MOBILE_RE.search(msg)
        if mobile:
            remember("mobile", mobile.group("val"), line)
            continue

        nic = ENTERED_NIC_RE.search(msg)
        if nic:
            remember("nic", nic.group("val"), line)
            continue

        account = ENTERED_ACCOUNT_RE.search(msg)
        if account:
            remember("account", account.group("val"), line)
            continue

        card = CARD_NO_RE.search(msg)
        if card and session is not None:
            session.card_no = card.group("card").strip()
            continue

        terminal = TERMINAL_RE.search(msg)
        if terminal and session is not None:
            session.terminal_id = terminal.group("tid").strip()
            continue

        # ---- account validation (the anchor) -------------------------------
        if VALIDATION_START_RE.match(msg):
            if session is None:
                session = open_session(line, implicit=True)
            stats.validations_seen += 1
            session.validation = parse_validation(buffer, index)
            continue

        # ---- cash acceptance ------------------------------------------------
        if CASH_IN_OK_RE.match(msg) or CASH_IN_FAIL_RE.match(msg):
            if session is None:
                session = open_session(line, implicit=True)
            stats.cash_in_events += 1
            accepted = extract_deposit_denominations(buffer, index, index + 60)
            if accepted:
                # Each "Accepting Cash In" prints the running escrow total, not
                # the notes just added, so the snapshots are kept in order and
                # the final one wins. Summing them would multiply a deposit
                # whose notes were refused and re-inserted.
                session.cash_in.setdefault("snapshots", []).append(accepted)
                session.cash_in["denominations"] = dict(accepted)
                session.cash_in["attempts"] = len(session.cash_in["snapshots"])
            session.cash_in["result"] = "SUCCESS" if CASH_IN_OK_RE.match(msg) else "FAILED"
            session.cash_in["datetime"] = line.timestamp
            continue

        if REFUSE_RE.search(msg) and session is not None:
            session.cash_in["refused"] = True
            for ahead in range(index + 1, min(len(buffer), index + 4)):
                reason = REASON_RE.match(buffer[ahead].msg or "")
                if reason:
                    session.cash_in["refuse_reason"] = reason.group("val").strip()
                    break
            continue

        if CASH_IN_ROLLBACK_RE.search(msg) and session is not None:
            session.cash_in["rollback"] = True
            continue

        parsed = _field(msg)
        if parsed and session is not None and parsed[0] in {"currency id", "currency"}:
            session.cash_in.setdefault("currency", parsed[1] or None)

        # ---- bill payment: biller verification (the anchor's context) -------
        if BILL_VERIFY_RE.match(msg):
            if session is None:
                session = open_session(line, implicit=True)
            session.bill_payment = parse_bill_verification(buffer, index)
            continue

        # ---- bill payment: confirm request (the record) ----------------------
        if BILL_CONFIRM_RE.match(msg):
            if session is None:
                session = open_session(line, implicit=True)
            records.append(parse_bill_payment(buffer, index, session, source_file, stats))
            _consume_cash_in(session)
            continue

        # ---- deposit request ------------------------------------------------
        if DEPOSIT_START_RE.match(msg):
            if session is None:
                session = open_session(line, implicit=True)
            records.append(parse_deposit(buffer, index, session, source_file, stats))
            _consume_cash_in(session)

    close_session()
    for record in records:
        if not record.get("TERMINAL_ID"):
            record["TERMINAL_ID"] = file_terminal
    return records


def _consume_cash_in(session: DepositContext) -> None:
    """
    Forget the accepted notes once they have been credited.

    The escrow is emptied when a deposit or bill payment completes, so a second
    request in the same session starts counting from zero - without this the
    first deposit's notes would be attached to the second one as well.
    """
    session.cash_in = {key: value for key, value in (session.cash_in or {}).items()
                       if key in {"currency"}}
    session.bill_payment = {}


# --------------------------------------------------------------------------- #
# 8b. Deposit DataFrame assembly
# --------------------------------------------------------------------------- #


def _build_deposit_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=DEPOSIT_FINAL_COLUMNS)

    df = pd.DataFrame(records)
    df["TRANSACTION_DATETIME"] = pd.to_datetime(df["TRANSACTION_DATETIME"], errors="coerce")
    df["VALIDATION_DATETIME"] = pd.to_datetime(df.get("VALIDATION_DATETIME"), errors="coerce")
    df["DATE"] = df["TRANSACTION_DATETIME"].dt.date
    df["TIME"] = df["TRANSACTION_DATETIME"].dt.time

    df = _expand_denomination_columns(df)

    for col in ("AMOUNT", "AMOUNT_RAW", "DENOM_AMOUNT", "DENOM_AMOUNT_DIFF",
                "DEPOSIT_THRESHOLD"):
        df[col] = pd.to_numeric(df.get(col), errors="coerce")

    # Keep these typed so the Greenplum columns come out as integer / boolean
    # rather than numeric / text (abandoned rows leave gaps in both).
    df["SOURCE_LINE"] = pd.to_numeric(df.get("SOURCE_LINE"), errors="coerce").astype("Int64")
    df["DEPOSIT_SEQ"] = pd.to_numeric(df.get("DEPOSIT_SEQ"), errors="coerce").astype("Int64")
    df["CASH_IN_ATTEMPTS"] = pd.to_numeric(df.get("CASH_IN_ATTEMPTS"),
                                           errors="coerce").astype("Int64")
    for col in ("NOTES_REFUSED", "CASH_IN_ROLLBACK"):
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)

    for col in ("ATM_NO", "DEPOSIT_TYPE", "ACCOUNT_NO", "ACCOUNT_MASKED", "ACCOUNT_TYPE",
                "CUSTOMER_NAME", "MOBILE_NO", "NIC_NO", "CARD_NO", "CURRENCY",
                "DEPOSIT_STATUS", "RESPONSE_CODE", "ACTION_CODE", "TRANSACTION_REF",
                "AUX_SEQ", "TRACE_ID", "TERMINAL_ID", "NARRATION", "CASH_IN_RESULT",
                "REFUSE_REASON", "RECEIPT_TYPE", "TRX_ERROR", "VALIDATION_RESPONSE_CODE",
                "VALIDATION_TRACE_ID", "VALIDATION_ACTION_CODE", "VALIDATION_REF",
                "SESSION_ID", "SOURCE_FILE", "AMOUNT_SOURCE", "BILLER_ID", "BILLER_NAME",
                "BILL_REFERENCE_NO"):
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip().replace({"": pd.NA})

    valid = ["SUCCESS", "FAILED", "UNKNOWN", "NO_DEPOSIT"]
    df["STATUS"] = (df["STATUS"].astype("string").str.upper()
                    .where(df["STATUS"].isin(valid), "UNKNOWN"))

    note_cols = df.attrs.get("note_columns", [])
    head = DEPOSIT_FINAL_COLUMNS[:DEPOSIT_FINAL_COLUMNS.index("DENOM_AMOUNT") + 1]
    tail = DEPOSIT_FINAL_COLUMNS[DEPOSIT_FINAL_COLUMNS.index("DENOM_AMOUNT") + 1:]
    ordered = head + note_cols + ["DENOM_MATCHES_AMOUNT"] + tail
    ordered += [c for c in df.columns if c not in ordered]
    df = df[[c for c in ordered if c in df.columns]]
    df.attrs["note_columns"] = note_cols
    return df.sort_values(["ATM_NO", "TRANSACTION_DATETIME"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 8c. Deposit entry points
# --------------------------------------------------------------------------- #


def process_single_deposit_file(path: str,
                                atm_no: Optional[str] = None,
                                include_abandoned: bool = True,
                                stats: Optional[DepositParseStats] = None) -> pd.DataFrame:
    """
    Parse ONE journal file and return its deposit-level DataFrame.

    ``atm_no`` defaults to the name of the folder holding the file, which is the
    same convention :func:`process_all_atms` uses.
    """
    stats = stats or DepositParseStats()
    atm = str(atm_no or os.path.basename(os.path.dirname(os.path.abspath(path))))

    records = extract_deposit_blocks(read_ejournal_file(path, stats), path, stats,
                                     include_abandoned=include_abandoned)
    for rec in records:
        rec["ATM_NO"] = atm
        rec["SESSION_ID"] = f"{atm}|{rec['SESSION_ID']}"
    stats.files_processed += 1

    df = _build_deposit_dataframe(records)
    df.attrs["stats"] = stats.as_dict()
    df.attrs["unparsed"] = _build_unparsed(records)
    return df


def process_atm_folder_deposits(folder: str,
                                atm_no: str,
                                stats: DepositParseStats,
                                include_abandoned: bool = True) -> List[Dict[str, Any]]:
    """Parse every journal file under one ATM folder for deposits."""
    records: List[Dict[str, Any]] = []
    paths = find_journal_files(folder)

    if not paths:
        stats.atm_folders_without_files += 1
        logger.warning("ATM %s: no journal files found under %s", atm_no, folder)
        return []

    for path in paths:
        try:
            lines = read_ejournal_file(path, stats)
            file_records = extract_deposit_blocks(lines, path, stats,
                                                  include_abandoned=include_abandoned)
            for rec in file_records:
                rec["ATM_NO"] = str(atm_no)
                rec["SESSION_ID"] = f"{atm_no}|{rec['SESSION_ID']}"
            records.extend(file_records)
            stats.files_processed += 1
            logger.info("ATM %s | %s -> %d deposit record(s)",
                        atm_no, os.path.basename(path), len(file_records))
        except Exception as exc:                      # never abort the run
            stats.files_failed += 1
            logger.exception("Failed to parse deposits in %s: %s", path, exc)

    return records


def process_all_atms_deposits(input_directory: str,
                              verbose: bool = True,
                              include_abandoned: bool = True) -> pd.DataFrame:
    """
    Parse every ATM folder under ``input_directory`` and return the deposit-level
    DataFrame. The withdrawal equivalent is :func:`process_all_atms`.
    """
    if verbose and not logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    stats = DepositParseStats()
    all_records: List[Dict[str, Any]] = []

    if not os.path.isdir(input_directory):
        raise NotADirectoryError(input_directory)

    entries = sorted(e for e in os.listdir(input_directory)
                     if os.path.isdir(os.path.join(input_directory, e)))
    if not entries:
        entries = [""]

    for entry in entries:
        folder = os.path.join(input_directory, entry) if entry else input_directory
        atm_no = entry or os.path.basename(os.path.normpath(input_directory))
        stats.atm_folders_processed += 1
        all_records.extend(process_atm_folder_deposits(folder, atm_no, stats,
                                                       include_abandoned=include_abandoned))

    df = _build_deposit_dataframe(all_records)
    df.attrs["stats"] = stats.as_dict()
    df.attrs["unparsed"] = _build_unparsed(all_records)

    if verbose:
        logger.info("Deposit summary: %s", stats.as_dict())
    return df


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
