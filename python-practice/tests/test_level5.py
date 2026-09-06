import unittest

from tests.support import load

m = load("level5_classes")


class Task13BankAccount(unittest.TestCase):
    """Task 13: BankAccount"""

    def test_stores_owner_and_balance(self):
        account = m.BankAccount("Ada", 100)
        self.assertEqual(account.owner, "Ada")
        self.assertEqual(account.balance, 100)

    def test_balance_defaults_to_zero(self):
        self.assertEqual(m.BankAccount("Ada").balance, 0)

    def test_deposit_and_withdraw(self):
        account = m.BankAccount("Ada", 100)
        account.deposit(50)
        account.withdraw(20)
        self.assertEqual(account.balance, 130)

    def test_overdraft_raises_value_error(self):
        account = m.BankAccount("Ada", 10)
        with self.assertRaises(ValueError):
            account.withdraw(20)

    def test_non_positive_amounts_raise_value_error(self):
        account = m.BankAccount("Ada", 10)
        with self.assertRaises(ValueError):
            account.deposit(0)
        with self.assertRaises(ValueError):
            account.withdraw(-5)


class Task14Stack(unittest.TestCase):
    """Task 14: Stack"""

    def test_push_pop_is_last_in_first_out(self):
        stack = m.Stack()
        stack.push(1)
        stack.push(2)
        self.assertEqual(stack.pop(), 2)
        self.assertEqual(stack.pop(), 1)

    def test_peek_does_not_remove(self):
        stack = m.Stack()
        stack.push("a")
        self.assertEqual(stack.peek(), "a")
        self.assertEqual(len(stack), 1)

    def test_len_and_is_empty(self):
        stack = m.Stack()
        self.assertTrue(stack.is_empty())
        self.assertEqual(len(stack), 0)
        stack.push(1)
        self.assertFalse(stack.is_empty())
        self.assertEqual(len(stack), 1)

    def test_pop_on_empty_raises_index_error(self):
        with self.assertRaises(IndexError):
            m.Stack().pop()

    def test_peek_on_empty_raises_index_error(self):
        with self.assertRaises(IndexError):
            m.Stack().peek()


class Task15SafeInt(unittest.TestCase):
    """Task 15: safe_int()"""

    def test_converts_numeric_strings(self):
        self.assertEqual(m.safe_int("42"), 42)

    def test_bad_string_returns_the_default(self):
        self.assertEqual(m.safe_int("abc"), 0)

    def test_custom_default(self):
        self.assertEqual(m.safe_int(None, -1), -1)

    def test_decimal_string_is_not_an_int(self):
        self.assertEqual(m.safe_int("7.9"), 0)

    def test_passes_through_real_ints(self):
        self.assertEqual(m.safe_int(7), 7)


if __name__ == "__main__":
    unittest.main()
