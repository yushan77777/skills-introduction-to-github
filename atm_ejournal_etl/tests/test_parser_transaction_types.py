"""
The journal grammars the parser learned after the first release.

Each test is built from the shape of a real journal, because that is what these
three features exist for:

* an **iWallet cash withdrawal** - cardless, no "Cash Withdraw Initiated", the
  amount logged in minor units;
* a **bill payment deposit** - cash accepted and paid to a biller, with no
  amount logged anywhere and a boolean outcome;
* a **rejected cash deposit** - notes refused and re-inserted inside one
  session, where the cash-in breakdown is logged once per insertion and only
  the last one is the deposit.

The fourth theme running through all of them is amount vs. denomination: the
value the host recorded and the value of the notes the machine handled are kept
side by side, and a disagreement is reported rather than patched over.
"""

from __future__ import annotations

import os

import pytest

from atm_ejournal_parser import (DepositParseStats, ParseStats, denomination_value,
                                 process_single_deposit_file, process_single_file,
                                 reconcile_amount, select_final_denominations)


def write_journal(path: str, lines) -> str:
    """Write ``[(hhmmss, message)]`` as a journal file and return the path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="latin-1") as handle:
        handle.write("\n".join(f"[01082026 {stamp} 000][001][INF]> {message}"
                               for stamp, message in lines) + "\n")
    return path


def cash_in(stamp: str, compact: str):
    """One 'Accepting Cash In Succeeded' block with its compact breakdown."""
    return [(stamp, "-Accepting Cash In Succeeded"),
            (stamp, f"----Denom      : {compact}"),
            (stamp, "-Getting Depositor Status ------------")]


# --------------------------------------------------------------------------- #
# iWallet cash withdrawal
# --------------------------------------------------------------------------- #


IWALLET = [
    ("135102", "-Denominate Command Execute-----------"),
    ("135102", "---Amount        : 1900"),
    ("135102", "---Denominate Execution Completed"),
    ("135102", "----Denomination"),
    ("135102", "-----CU  TYP  VALUE   NUM"),
    ("135102", "-----01  RCY  005000  000"),
    ("135102", "-----02  RCY  001000  001"),
    ("135102", "-----03  RCY  000500  001"),
    ("135102", "-----04  RCY  000100  004"),
    ("135102", "-iWallet Cash Withdraw"),
    ("135102", "---Account Number      : "),
    ("135102", "---Trx Amount          : 190000"),
    ("135102", "---Bank Code           : 6278"),
    ("135102", "---Ret Ref No          : 723314775978"),
    ("135102", "---Terminal ID         : A0000131"),
    ("135102", "---Fee                 : D00000000"),
    ("135102", "---Wallet ID           : SRANJALA"),
    ("135102", "---Aux Number          : A000013120260821135102-01"),
    ("135102", "-iWallet Cash Withdraw Successful"),
    ("135102", "---RESP_CODE           : 000"),
    ("135102", "---ACT_CODE            : suc"),
    ("135102", "---TRACE_NO            : 592498"),
    ("135102", "-Dispense Command Executed -----------"),
    ("135102", "----Amount   : 1900"),
    ("135102", "----Mix      : 1"),
    ("135102", "----Currency : LKR"),
    ("135110", "----Denomination"),
    ("135110", "-----CU  TYP  VALUE   NUM"),
    ("135110", "-----02  RCY  001000  001"),
    ("135110", "-----03  RCY  000500  001"),
    ("135110", "-----04  RCY  000100  004"),
    ("135110", "---Dispense Succeeded"),
    ("135115", "---Present Succeeded"),
    ("135122", "---Cash Has Taken"),
    ("135127", "-Close Session -----------------------"),
    ("135127", "#SESSION-END#"),
]


@pytest.fixture
def wallet_row(tmp_path):
    path = write_journal(str(tmp_path / "ATM999" / "EJ_IWALLET.TXT"), IWALLET)
    frame = process_single_file(path, atm_no="ATM999")
    assert len(frame) == 1
    return frame.iloc[0]


def test_a_wallet_withdrawal_is_an_ordinary_withdrawal_row(wallet_row):
    assert wallet_row["TRANSACTION_TYPE"] == "IWALLET_WITHDRAWAL"
    assert wallet_row["STATUS"] == "SUCCESS"            # from the terminator suffix
    assert bool(wallet_row["CASH_TAKEN"]) is True
    assert wallet_row["DISPENSE_RESULT"] == "SUCCESS"
    assert wallet_row["CURRENCY"] == "LKR"


def test_the_wallet_amount_is_logged_in_cents(wallet_row):
    """``Trx Amount : 190000`` is 1,900.00 - and the notes prove the scale."""
    assert wallet_row["AMOUNT"] == 1900.0
    assert wallet_row["AMOUNT_RAW"] == 190000.0
    assert wallet_row["AMOUNT_SOURCE"] == "REQUEST_SCALED"
    assert wallet_row["DENOM_AMOUNT"] == 1900.0
    assert wallet_row["DENOM_AMOUNT_DIFF"] == 0.0
    assert bool(wallet_row["DENOM_MATCHES_AMOUNT"]) is True


def test_the_wallet_identity_is_captured(wallet_row):
    assert wallet_row["WALLET_ID"] == "SRANJALA"
    assert wallet_row["BANK_CODE"] == "6278"
    assert wallet_row["RET_REF_NO"] == "723314775978"
    assert wallet_row["FEE"] == "D00000000"
    assert wallet_row["TERMINAL_ID"] == "A0000131"
    assert wallet_row["TRANSACTION_REF"] == "A000013120260821135102-01"
    # Printed as TRACE_NO / RESP_CODE / ACT_CODE after the terminator.
    assert wallet_row["TRACE_ID"] == "592498"
    assert wallet_row["RESPONSE_CODE"] == "000"
    assert wallet_row["ACTION_CODE"] == "suc"


def test_the_dispensed_notes_are_the_breakdown(wallet_row):
    assert wallet_row["DENOMINATION"] == "1000x1 + 500x1 + 100x4"
    assert wallet_row["NOTES_COUNT"] == 6
    assert wallet_row["DISPENSED_AMOUNT"] == 1900.0
    assert wallet_row["PLANNED_DENOMINATION"] == "1000x1 + 500x1 + 100x4"


def test_a_declined_wallet_withdrawal_is_failed_and_dispenses_nothing(tmp_path):
    """A declined wallet withdrawal ends at its terminator: no dispense at all."""
    cut = IWALLET.index(("135102", "-Dispense Command Executed -----------"))
    lines = [(stamp, msg.replace("iWallet Cash Withdraw Successful",
                                 "iWallet Cash Withdraw Failed")
                       .replace("RESP_CODE           : 000", "RESP_CODE           : 116")
                       .replace("ACT_CODE            : suc", "ACT_CODE            : dcl"))
             for stamp, msg in IWALLET[:cut]]
    lines += [("135105", "-Close Session -----------------------"), ("135105", "#SESSION-END#")]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_DECLINED.TXT"), lines)

    row = process_single_file(path, atm_no="ATM999").iloc[0]

    assert row["STATUS"] == "FAILED"
    assert row["RESPONSE_CODE"] == "116"
    assert row["ACTION_CODE"] == "dcl"
    # Nothing was dispensed, so there are no notes to settle the scale with and
    # the configured minor-unit divisor is what decides.
    assert row["AMOUNT"] == 1900.0
    assert row["AMOUNT_SOURCE"] == "REQUEST_SCALED"
    assert str(row["DENOMINATION"]) in {"None", "<NA>", "nan"}
    assert row["CASH_TAKEN"] is None or str(row["CASH_TAKEN"]) == "None"


# --------------------------------------------------------------------------- #
# Bill payment deposit
# --------------------------------------------------------------------------- #


BILL_PAYMENT = [
    ("171007", "Entered Mobile No: 0758994275"),
    ("171015", "Entered NIC :941693164V"),
    *cash_in("171103", "000100-000004:000500-000003"),
    ("171106", "-Cardless BillPayment - Biller Verification"),
    ("171106", "----Biller Data(ID/DESC) : CEB Only/CEB Only"),
    ("171106", "----AUX NO : 666 :xx:A000011120260801171106-01"),
    ("171106", "-----Function Status    : True"),
    ("171106", "-----Trace ID           : 364042"),
    ("171106", "-Ending Cash In Process---------------"),
    ("171114", "-Ending Cash In Succeeded"),
    ("171114", "-Cardless Bill Payment Confirm Request"),
    ("171114", "----AUX NO : 667 :xx:A000011120260801171114-02"),
    ("171114", "---Cardless Bill Payment Confirm Completed"),
    ("171114", "-----Function Status    : True"),
    ("171114", "-----Trace ID           : 364027"),
    ("171114", "-----Ret Referenece No  : BP00099"),
    ("171114", "-Print Command Executed --------------"),
    ("171114", "----Reciept Type : CARDLESS_BILL_PAYMENT"),
    ("171125", "---Terminal ID         : A0000111"),
    ("171126", "-Close Session -----------------------"),
    ("171126", "#SESSION-END#"),
]


@pytest.fixture
def bill_row(tmp_path):
    path = write_journal(str(tmp_path / "ATM999" / "EJ_BILL.TXT"), BILL_PAYMENT)
    frame = process_single_deposit_file(path, atm_no="ATM999")
    assert len(frame) == 1
    return frame.iloc[0]


def test_a_bill_payment_is_a_deposit_row(bill_row):
    assert bill_row["DEPOSIT_TYPE"] == "BILL_PAYMENT"
    assert bill_row["STATUS"] == "SUCCESS"              # Function Status : True
    assert bill_row["RECEIPT_TYPE"] == "CARDLESS_BILL_PAYMENT"
    assert bill_row["DEPOSIT_SEQ"] == 1


def test_the_notes_accepted_are_the_bill_payment_amount(bill_row):
    """Nothing in the bill payment legs logs an amount - the cash is the amount."""
    assert bill_row["AMOUNT"] == 1900.0
    assert bill_row["AMOUNT_SOURCE"] == "DENOMINATION"
    assert bill_row["DENOMINATION"] == "500x3 + 100x4"
    assert bill_row["NOTES_COUNT"] == 7
    assert bill_row["DENOM_AMOUNT"] == 1900.0
    assert bool(bill_row["DENOM_MATCHES_AMOUNT"]) is True


def test_the_biller_and_the_customer_are_captured(bill_row):
    assert bill_row["BILLER_ID"] == "CEB Only"
    assert bill_row["BILLER_NAME"] == "CEB Only"
    assert bill_row["BILL_REFERENCE_NO"] == "BP00099"
    assert bill_row["MOBILE_NO"] == "0758994275"
    assert bill_row["NIC_NO"] == "941693164V"
    assert bill_row["TERMINAL_ID"] == "A0000111"
    assert bill_row["TRACE_ID"] == "364027"             # the confirm leg
    assert bill_row["VALIDATION_TRACE_ID"] == "364042"  # the verification leg
    assert bill_row["TRANSACTION_REF"] == "A000011120260801171114-02"


def test_a_failed_bill_payment_is_failed(tmp_path):
    lines = [(stamp, msg.replace("Function Status    : True", "Function Status    : False"))
             for stamp, msg in BILL_PAYMENT]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_BILL_NG.TXT"), lines)

    row = process_single_deposit_file(path, atm_no="ATM999").iloc[0]

    assert row["STATUS"] == "FAILED"
    assert row["AMOUNT"] == 1900.0                      # the cash was still taken


def test_a_verified_bill_payment_that_is_never_confirmed_is_still_reported(tmp_path):
    """Cash in the machine with no credit is what reconciliation needs to see."""
    lines = [line for line in BILL_PAYMENT if "Confirm" not in line[1]]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_BILL_ABANDONED.TXT"), lines)

    frame = process_single_deposit_file(path, atm_no="ATM999")
    row = frame.iloc[0]

    assert len(frame) == 1
    assert row["STATUS"] == "NO_DEPOSIT"
    assert row["DEPOSIT_TYPE"] == "BILL_PAYMENT"
    assert row["BILLER_ID"] == "CEB Only"
    assert row["DENOM_AMOUNT"] == 1900.0                # the notes are still reported
    assert frame.attrs["stats"]["cash_in_without_deposit"] == 1


# --------------------------------------------------------------------------- #
# Credit card payment: the same flow with a card in place of the biller
# --------------------------------------------------------------------------- #


CARD_PAYMENT = [
    ("171200", "Entered Mobile No: 0758994275"),
    *cash_in("171210", "005000-000002:001000-000003"),
    ("171215", "-Cardless Credit Card Payment - Card Verification"),
    ("171215", "----Card No             : 413541******1241"),
    ("171215", "----AUX NO : 701 :xx:A000011120260801171215-01"),
    ("171215", "-----Function Status    : True"),
    ("171215", "-----Trace ID           : 370011"),
    ("171218", "-Ending Cash In Process---------------"),
    ("171220", "-Ending Cash In Succeeded"),
    ("171222", "-Cardless Credit Card Payment Confirm Request"),
    ("171222", "----AUX NO : 702 :xx:A000011120260801171222-02"),
    ("171222", "---Cardless Credit Card Payment Confirm Completed"),
    ("171222", "-----Function Status    : True"),
    ("171222", "-----Trace ID           : 370025"),
    ("171222", "-----Ret Referenece No  : CC00123"),
    ("171223", "-Print Command Executed --------------"),
    ("171223", "----Reciept Type : CARDLESS_CREDIT_CARD_PAYMENT"),
    ("171230", "---Terminal ID         : A0000111"),
    ("171231", "-Close Session -----------------------"),
    ("171231", "#SESSION-END#"),
]


@pytest.fixture
def card_payment_row(tmp_path):
    path = write_journal(str(tmp_path / "ATM999" / "EJ_CARD_PAY.TXT"), CARD_PAYMENT)
    frame = process_single_deposit_file(path, atm_no="ATM999")
    assert len(frame) == 1
    return frame.iloc[0]


def test_a_credit_card_payment_is_its_own_deposit_type(card_payment_row):
    assert card_payment_row["DEPOSIT_TYPE"] == "CREDIT_CARD_PAYMENT"
    assert card_payment_row["STATUS"] == "SUCCESS"
    assert card_payment_row["RECEIPT_TYPE"] == "CARDLESS_CREDIT_CARD_PAYMENT"
    assert card_payment_row["MOBILE_NO"] == "0758994275"
    assert card_payment_row["TRACE_ID"] == "370025"
    assert card_payment_row["BILL_REFERENCE_NO"] == "CC00123"


def test_the_settled_card_is_captured(card_payment_row):
    assert card_payment_row["CARD_NO"] == "413541******1241"
    assert card_payment_row["ACCOUNT_MASKED"] == "413541******1241"


def test_the_cash_accepted_is_the_card_payment_amount(card_payment_row):
    assert card_payment_row["AMOUNT"] == 13000.0          # 5000x2 + 1000x3
    assert card_payment_row["AMOUNT_SOURCE"] == "DENOMINATION"
    assert card_payment_row["DENOMINATION"] == "5000x2 + 1000x3"
    assert card_payment_row["NOTES_COUNT"] == 5
    assert bool(card_payment_row["DENOM_MATCHES_AMOUNT"]) is True


def test_a_shorter_firmware_wording_is_still_recognised(tmp_path):
    """No "Cardless" prefix, no "Request" suffix - the role is what matters."""
    lines = [(stamp, msg.replace("Cardless Credit Card Payment - Card Verification",
                                 "Credit Card Payment Verification")
                       .replace("Cardless Credit Card Payment Confirm Request",
                                "CreditCard Payment Confirm")
                       .replace("Cardless Credit Card Payment Confirm Completed",
                                "CreditCard Payment Confirmation Succeeded"))
             for stamp, msg in CARD_PAYMENT]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_CARD_PAY2.TXT"), lines)

    row = process_single_deposit_file(path, atm_no="ATM999").iloc[0]

    assert row["DEPOSIT_TYPE"] == "CREDIT_CARD_PAYMENT"
    assert row["STATUS"] == "SUCCESS"
    assert row["AMOUNT"] == 13000.0


def test_an_amount_logged_by_the_payment_leg_is_used_and_reconciled(tmp_path):
    """When the leg does log an amount, it is the amount - cents and all."""
    lines = list(CARD_PAYMENT)
    position = lines.index(("171222", "----AUX NO : 702 :xx:A000011120260801171222-02"))
    lines.insert(position, ("171222", "-----Amount : 1300000"))
    path = write_journal(str(tmp_path / "ATM999" / "EJ_CARD_PAY3.TXT"), lines)

    row = process_single_deposit_file(path, atm_no="ATM999").iloc[0]

    assert row["AMOUNT"] == 13000.0
    assert row["AMOUNT_RAW"] == 1300000.0
    assert row["AMOUNT_SOURCE"] == "REQUEST_SCALED"
    assert row["DENOM_AMOUNT_DIFF"] == 0.0


def test_a_declined_credit_card_payment_keeps_the_cash_on_the_row(tmp_path):
    lines = [(stamp, msg.replace("Confirm Completed", "Confirm Failed")
                       .replace("Function Status    : True", "Function Status    : False"))
             for stamp, msg in CARD_PAYMENT]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_CARD_PAY_NG.TXT"), lines)

    row = process_single_deposit_file(path, atm_no="ATM999").iloc[0]

    assert row["DEPOSIT_TYPE"] == "CREDIT_CARD_PAYMENT"
    assert row["STATUS"] == "FAILED"
    assert row["DENOM_AMOUNT"] == 13000.0


def test_a_credit_card_bill_payment_is_a_card_payment_not_a_bill(tmp_path):
    """Wording that matches both families resolves to the more specific one."""
    lines = [(stamp, msg.replace("Cardless Credit Card Payment", "Credit Card Bill Payment"))
             for stamp, msg in CARD_PAYMENT]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_CC_BILL.TXT"), lines)

    row = process_single_deposit_file(path, atm_no="ATM999").iloc[0]

    assert row["DEPOSIT_TYPE"] == "CREDIT_CARD_PAYMENT"


def test_both_payment_types_and_a_deposit_coexist_in_one_file(tmp_path):
    lines = (BILL_PAYMENT + CARD_PAYMENT)
    path = write_journal(str(tmp_path / "ATM999" / "EJ_MIXED.TXT"), lines)

    frame = process_single_deposit_file(path, atm_no="ATM999")

    assert list(frame["DEPOSIT_TYPE"]) == ["BILL_PAYMENT", "CREDIT_CARD_PAYMENT"]
    assert list(frame["AMOUNT"]) == [1900.0, 13000.0]   # no shared escrow
    stats = frame.attrs["stats"]
    assert stats["bill_payment_records_detected"] == 1
    assert stats["credit_card_payment_records_detected"] == 1


# --------------------------------------------------------------------------- #
# Rejected cash deposit: the final accepted breakdown only
# --------------------------------------------------------------------------- #


REJECTED_DEPOSIT = [
    ("170548", "-Create Session-----------------------"),
    ("170544", "-----Card Number : 413541******1241"),
    # Attempt 1: a note is refused, 3 notes are held -> 7,000
    ("170620", "-Refuse Items Found"),
    ("170620", "---Reason : CIM_INVALID_BILL"),
    *cash_in("170621", "001000-000002:005000-000001"),
    ("170629", "-Items Taken"),
    # Attempt 2: refused again, the escrow total is logged again unchanged
    ("170701", "-Refuse Items Found"),
    ("170701", "---Reason : CIM_INVALID_BILL"),
    *cash_in("170702", "001000-000002:005000-000001"),
    ("170710", "-Items Taken"),
    # Attempt 3: accepted -> the escrow now holds 12,000, and that is credited
    *cash_in("170743", "001000-000002:005000-000002"),
    ("170746", "-Ending Cash In Process---------------"),
    ("170752", "-Ending Cash In Succeeded"),
    ("170752", "-Cash Deposit Request--------------"),
    ("170752", "-----A/C # : 122855**3666"),
    ("170752", "-----A/C Type : ps"),
    ("170752", "-----Amount : 1200000"),
    ("170752", "-----Status : deposit_ok"),
    ("170752", "----AUX NO : 661 :xx:A000011120260801170752-02"),
    ("170753", "-----Cash Deposit Status : OK"),
    ("170753", "-----Response        : 000"),
    ("170753", "-----Trace ID        : 361500"),
    ("170753", "---Cash Deposit Request Completed"),
    ("170753", "----Reciept Type : CASH_DEPOSIT"),
    ("170816", "#SESSION-END#"),
]


@pytest.fixture
def rejected(tmp_path):
    path = write_journal(str(tmp_path / "ATM999" / "EJ_REJECT.TXT"), REJECTED_DEPOSIT)
    return process_single_deposit_file(path, atm_no="ATM999")


def test_only_the_final_accepted_breakdown_is_the_deposit(rejected):
    """Summing the attempts would report 26,000 for a 12,000 deposit."""
    row = rejected.iloc[0]

    assert len(rejected) == 1
    assert row["AMOUNT"] == 12000.0
    assert row["DENOMINATION"] == "5000x2 + 1000x2"
    assert row["NOTES_COUNT"] == 4
    assert row["DENOM_AMOUNT"] == 12000.0
    assert row["DENOM_AMOUNT_DIFF"] == 0.0
    assert bool(row["DENOM_MATCHES_AMOUNT"]) is True


def test_the_refused_attempts_are_reported_not_hidden(rejected):
    row = rejected.iloc[0]

    assert row["CASH_IN_ATTEMPTS"] == 3
    assert bool(row["NOTES_REFUSED"]) is True
    assert row["REFUSE_REASON"] == "CIM_INVALID_BILL"
    stats = rejected.attrs["stats"]
    assert stats["cash_in_events"] == 3
    assert stats["superseded_cash_in_snapshots"] == 2
    assert stats["denomination_amount_mismatches"] == 0


def test_the_breakdown_that_matches_the_amount_wins_over_the_last_one(tmp_path):
    """A status line logged after the credited one must not become the deposit."""
    lines = list(REJECTED_DEPOSIT)
    position = lines.index(("170746", "-Ending Cash In Process---------------"))
    lines[position:position] = cash_in("170744", "001000-000002:005000-000003")  # 17,000
    path = write_journal(str(tmp_path / "ATM999" / "EJ_LATE.TXT"), lines)

    row = process_single_deposit_file(path, atm_no="ATM999").iloc[0]

    assert row["AMOUNT"] == 12000.0
    assert row["DENOM_AMOUNT"] == 12000.0               # not the 17,000 logged last
    assert row["CASH_IN_ATTEMPTS"] == 4


def test_two_deposits_in_one_session_do_not_share_their_notes(tmp_path):
    lines = [
        ("100000", "-Create Session-----------------------"),
        *cash_in("100010", "005000-000002"),
        ("100012", "-Cardless Cash Deposit Request--------------"),
        ("100012", "-----Amount : 1000000"),
        ("100012", "-----Cardless Cash Deposit Status : OK"),
        ("100013", "---Cardless Cash Deposit Request Completed"),
        *cash_in("100020", "001000-000003"),
        ("100022", "-Cardless Cash Deposit Request--------------"),
        ("100022", "-----Amount : 300000"),
        ("100022", "-----Cardless Cash Deposit Status : OK"),
        ("100023", "---Cardless Cash Deposit Request Completed"),
        ("100030", "-Close Session -----------------------"),
    ]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_TWO.TXT"), lines)

    frame = process_single_deposit_file(path, atm_no="ATM999")

    assert list(frame["AMOUNT"]) == [10000.0, 3000.0]
    assert list(frame["DENOMINATION"]) == ["5000x2", "1000x3"]
    assert [bool(value) for value in frame["DENOM_MATCHES_AMOUNT"]] == [True, True]


# --------------------------------------------------------------------------- #
# Amount vs. denomination
# --------------------------------------------------------------------------- #


def test_an_unexplained_difference_is_reported_not_corrected(tmp_path, caplog):
    lines = [
        ("140000", "-Create Session-----------------------"),
        *cash_in("140010", "001000-000002"),                 # 2,000 accepted
        ("140020", "-Cash Deposit Request--------------"),
        ("140020", "-----Amount : 500000"),                  # 5,000 claimed
        ("140020", "-----Cash Deposit Status : OK"),
        ("140021", "---Cash Deposit Request Completed"),
        ("140030", "-Close Session -----------------------"),
    ]
    path = write_journal(str(tmp_path / "ATM999" / "EJ_DIFF.TXT"), lines)

    frame = process_single_deposit_file(path, atm_no="ATM999")
    row = frame.iloc[0]

    assert row["AMOUNT"] == 5000.0                      # the host's number is kept
    assert row["DENOM_AMOUNT"] == 2000.0                # so is the machine's
    assert row["DENOM_AMOUNT_DIFF"] == -3000.0
    assert bool(row["DENOM_MATCHES_AMOUNT"]) is False
    assert frame.attrs["stats"]["denomination_amount_mismatches"] == 1
    assert "does not match the notes accepted" in caplog.text


def test_reconcile_amount_lets_the_notes_decide_the_scale():
    # cents, confirmed by the notes
    assert reconcile_amount(190000, 1900.0, minor_units=True) == (1900.0, "REQUEST_SCALED")
    # the same firmware field already in rupees - not divided a second time
    assert reconcile_amount(1900, 1900.0, minor_units=True) == (1900.0, "REQUEST")
    # no notes to compare against: the configured scale is trusted
    assert reconcile_amount(190000, None, minor_units=True) == (1900.0, "REQUEST_SCALED")
    assert reconcile_amount(50000, None) == (50000.0, "REQUEST")
    # no amount logged at all: the notes are the amount
    assert reconcile_amount(None, 1900.0) == (1900.0, "DENOMINATION")
    assert reconcile_amount(None, None) == (None, None)


def test_select_final_denominations_prefers_the_amount_then_the_last_snapshot():
    snapshots = [{1000: 2}, {1000: 2, 5000: 2}, {1000: 2, 5000: 3}]
    cash_in_state = {"snapshots": snapshots}

    chosen, attempts = select_final_denominations(cash_in_state)
    assert chosen == {1000: 2, 5000: 3} and attempts == 3      # the last one

    chosen, _ = select_final_denominations(cash_in_state, candidate_amounts=(12000,))
    assert denomination_value(chosen) == 12000.0               # the matching one

    chosen, _ = select_final_denominations(cash_in_state, candidate_amounts=(99,))
    assert chosen == {1000: 2, 5000: 3}                        # no match -> the last

    assert select_final_denominations({}) == ({}, 0)


# --------------------------------------------------------------------------- #
# Nothing that already worked may move
# --------------------------------------------------------------------------- #


def test_a_card_withdrawal_is_unchanged(etl_home, input_tree):
    """The ordinary withdrawal row the ETL has always loaded."""
    path = input_tree["files"][0]
    stats = ParseStats()

    frame = process_single_file(path, atm_no="ATM001", stats=stats)
    row = frame.iloc[0]

    assert row["TRANSACTION_TYPE"] == "WITHDRAWAL"
    assert row["AMOUNT"] == 50000.0
    assert row["AMOUNT_SOURCE"] == "REQUEST"            # never rescaled
    assert row["STATUS"] == "SUCCESS"
    assert row["DENOMINATION"] == "5000x10"
    assert bool(row["DENOM_MATCHES_AMOUNT"]) is True
    assert str(row["WALLET_ID"]) in {"None", "<NA>", "nan"}
    assert stats.iwallet_records_detected == 0


def test_a_journal_with_no_new_grammar_produces_no_deposit_rows(etl_home, input_tree):
    stats = DepositParseStats()

    frame = process_single_deposit_file(input_tree["files"][0], atm_no="ATM001", stats=stats)

    assert frame.empty
    assert stats.bill_payment_records_detected == 0
