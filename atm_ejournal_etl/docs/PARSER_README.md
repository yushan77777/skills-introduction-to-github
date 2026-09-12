# ATM E-Journal Withdrawal Parser

Parses ATM electronic journal files into a single withdrawal-level pandas DataFrame
(`df`), one logical withdrawal per row.

## Contents

| File | Purpose |
| --- | --- |
| `atm_ejournal_parser.py` | The parser module — import it or run it from the CLI |
| `run_atm_ejournal_parser.ipynb` | Manual runner: run, audit, profile, debug, export |
| `requirements.txt` | Dependencies (`pandas`, plus `openpyxl` for Excel export) |

## Input layout

The folder name is used as `ATM_NO`:

```
ATM_EJOURNALS/
  A0023011/
    EJOURNAL_10092026_00.TXT
    EJOURNAL_11092026_00.TXT
  A0023012/
    EJOURNAL_10092026_00.TXT
```

Files are picked up recursively (symlinked folders included, any depth of subfolder).
Known extensions — `.txt`, `.log`, `.jrn`, `.dat`, `.ej`, `.ejn`, `.jnl`, `.prn` — are
taken outright; any other file that is not on `SKIP_EXTENSIONS` is opened and checked
for journal header lines, so `EJ_20260910`, `JOURNAL.001` and extensionless files are
still parsed. A flat directory of journal files also works.

### An ATM is missing from the results?

```python
from atm_ejournal_parser import scan_input_tree

tree = scan_input_tree("ATM_EJOURNALS")
tree.groupby("ATM_FOLDER")["WILL_PARSE"].sum()      # files found per ATM
tree[~tree["WILL_PARSE"]][["ATM_FOLDER", "FILE", "REASON"]]   # and why the rest were skipped
```

The run also logs one line per ATM folder (`ATM A0023011: 2 file(s), 118 attempts ->
116 rows`) and warns about any folder that yielded no journal files. `ATM_NO` comes from
the **top-level** folder name, so if your tree is `ROOT/2026-09-10/A0023011/...` the date
becomes the ATM number — point `INPUT_DIR` one level deeper, or restructure.

## Quick start

```bash
pip install -r requirements.txt
python atm_ejournal_parser.py ATM_EJOURNALS --csv withdrawals.csv
```

```python
from atm_ejournal_parser import process_all_atms

df = process_all_atms("ATM_EJOURNALS")
df.attrs["stats"]      # audit counters
df.attrs["unparsed"]   # low-confidence blocks, raw text retained
```

Or open `run_atm_ejournal_parser.ipynb`, set `INPUT_DIR` in the second cell and run
top to bottom.

## Journal grammar the parser relies on

```
[DDMMYYYY HHMMSS mmm][dev][LVL]> <message>

===================== Trx Started =====================      <- card session
    -----Card Number : 539157******0717
    #SESSION-START#
    #TRANSACTION-START#                                      <- one logical txn
        -Amount Requsted --------------------
        ---Amount        : 50000                             <- customer-keyed amount
        -Cash Withdraw Initiated -------------                <- withdrawal block
        -----Amount : 50000
        ----AUX NO : 2868 :xx:A002301120260910064244-02
        -----Withdraw Status : OK                            <- OK | Failed
        -----Account         : 539157XX..XX0717
        -----Response        : 000
        -----Trace ID        : 559198
        ---Cash Withdraw Initiated Completed                 <- or ...Initiated Fail
        -Dispense Command Executed -----------
        ----Denomination
        -----CU  TYP  VALUE   NUM
        -----02  RCY  005000  001                            <- notes actually paid
        -----03  RCY  001000  005
        ---Dispense Succeeded
        ---Present Succeeded / ---Cash Has Taken
    #TRANSACTION-END#
============== Trx End:CardRemoved:9/10/2026 6:43:04 AM=============
```

Notes derived from real files:

* The header stamp is **day-first** (`DDMMYYYY`), confirmed against the human-readable
  `Trx End:CardRemoved:<M/D/YYYY h:mm:ss AM>` footer in the same session.
* A `#TRANSACTION-START#`/`#TRANSACTION-END#` block holds at most one withdrawal
  attempt, so retries appear as consecutive transaction blocks in one card session.
* Failed attempts leave `Account` blank, so the masked PAN from `Card Number` is the
  reliable customer key for grouping attempts.
* The note breakdown that was paid out is the denomination table inside the
  `Dispense Command Executed` block. The earlier `Denominate Execution Completed`
  table is only the mix the ATM *planned* and differs most of the time, so it is
  kept separately as `PLANNED_DENOMINATION`.
* Parsing is pattern-driven (keywords, field names, block boundaries). Decorative
  dashes and column alignment vary between firmware versions and are stripped, so
  no logic depends on fixed line positions.

## Repeated-attempt rule

A retry chain is a run of **consecutive** attempts inside one card session sharing the
same card and the same amount. A SUCCESS closes the chain — cash left the machine — so
three successful 2,500 withdrawals on one card stay three rows.

Within a chain:

* keep the first FAILED attempt
* keep the final SUCCESS attempt
* drop intermediate FAILED attempts
* all-failed chains keep the first and, with `keep_last_failure=True` (default), the last

`ATTEMPT_NO`, `ATTEMPT_COUNT` and `IS_RETRY` record what each surviving row came from.

### Optional: link re-keyed amounts

A `2000 FAILED` followed 31 seconds later by `1500 SUCCESS` on the same card is arguably
one withdrawal where the customer lowered the amount. Strict amount matching keeps them
separate by default. To chain them instead:

```python
df = process_all_atms("ATM_EJOURNALS",
                      link_failed_across_amounts=True,
                      retry_window_seconds=180)
```

## Note denominations

`DENOMINATION` reads like `5000x1 + 1000x5` for a 10,000 withdrawal. Alongside it:

| Column | Meaning |
| --- | --- |
| `DENOMINATION` | Readable breakdown, highest note first |
| `DENOM_BREAKDOWN` | The same as a dict, e.g. `{5000: 1, 1000: 5}` |
| `NOTES_COUNT` | Total notes handed over |
| `NOTES_5000`, `NOTES_1000`, ... | One column per note value found in the data |
| `DENOM_AMOUNT` | Cash value implied by the notes |
| `DENOM_MATCHES_AMOUNT` | `True` when that value equals `AMOUNT` |
| `PLANNED_DENOMINATION` | The mix the ATM planned, for comparison |
| `MIX_NUMBER` | Dispenser mix profile used |

For a long-format view (one row per note value):

```python
from atm_ejournal_parser import explode_denominations

notes = explode_denominations(df)
notes.groupby(["ATM_NO", "NOTE_VALUE"])[["NOTE_COUNT", "NOTE_AMOUNT"]].sum()
```

Failed attempts have no denomination — nothing was dispensed — so these columns are
`NaN`/`<NA>` for them.

## Output columns

`ATM_NO`, `TRANSACTION_DATETIME`, `DATE`, `TIME`, `ACCOUNT_NO`, `CARD_NO`, `AMOUNT`,
`REQUESTED_AMOUNT`, `CURRENCY`, `STATUS`, `RESPONSE_CODE`, `TRANSACTION_REF`, `TRACE_ID`,
`TERMINAL_ID`, `CARD_SCHEME`, `DISPENSE_RESULT`, `DENOMINATION`, `NOTES_COUNT`,
`DENOM_AMOUNT`, `NOTES_<value>` columns, `DENOM_MATCHES_AMOUNT`, `CASH_TAKEN`,
`TRX_ERROR`, `ATTEMPT_NO`,
`ATTEMPT_COUNT`, `IS_RETRY`, `SESSION_ID`, `TXN_SEQ`, `SOURCE_FILE`, `SOURCE_LINE`, plus
`RESPONSE_DATETIME`, `ACTION_CODE`, `AUX_SEQ`, `FAST_CASH`, `DISPENSED_AMOUNT`,
`SOURCE_PATH`, `PARSE_CONFIDENT`, `DENOM_BREAKDOWN`, `PLANNED_DENOMINATION`, `MIX_NUMBER`.

`STATUS` is one of `SUCCESS`, `FAILED`, `UNKNOWN`. `AMOUNT` is numeric; account, card and
ATM numbers stay strings so leading zeros and masking survive. Unavailable fields are
`None`/`NaN` — never invented.

## Auditability

`df.attrs["stats"]` carries: ATM folders processed, files processed and failed, lines
read, unrecognised lines, card sessions, withdrawal records detected, successes,
failures, unknown-status records, withdrawals with a denomination captured, successful
withdrawals missing one, denomination/amount mismatches, repeats removed, and unparsed
records. Reconcile with:

```
withdrawal_records_detected - removed_repeat_attempts == len(df)
```

Blocks that could not be confidently parsed are kept in `df.attrs["unparsed"]` with the
raw text and source line, rather than being dropped. A file that raises never aborts the
run — it is logged and counted in `files_failed`.
