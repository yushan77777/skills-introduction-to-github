import unittest

from tests.support import load

m = load("level4_logic_recursion")


class Task10IsPalindrome(unittest.TestCase):
    """Task 10: is_palindrome()"""

    def test_ignores_punctuation_and_case(self):
        self.assertTrue(m.is_palindrome("A man, a plan, a canal: Panama"))

    def test_rejects_non_palindromes(self):
        self.assertFalse(m.is_palindrome("hello"))

    def test_empty_string_is_a_palindrome(self):
        self.assertTrue(m.is_palindrome(""))

    def test_digits_count(self):
        self.assertTrue(m.is_palindrome("12321"))


class Task11Fib(unittest.TestCase):
    """Task 11: fib()"""

    def test_base_cases(self):
        self.assertEqual(m.fib(0), 0)
        self.assertEqual(m.fib(1), 1)

    def test_small_values(self):
        self.assertEqual([m.fib(i) for i in range(8)], [0, 1, 1, 2, 3, 5, 8, 13])

    def test_is_fast_enough_for_larger_n(self):
        self.assertEqual(m.fib(30), 832040)


class Task12Flatten(unittest.TestCase):
    """Task 12: flatten()"""

    def test_flattens_deep_nesting(self):
        self.assertEqual(m.flatten([1, [2, [3, [4]], 5]]), [1, 2, 3, 4, 5])

    def test_already_flat(self):
        self.assertEqual(m.flatten([1, 2, 3]), [1, 2, 3])

    def test_empty_and_nested_empty_lists(self):
        self.assertEqual(m.flatten([]), [])
        self.assertEqual(m.flatten([[], [[]]]), [])

    def test_keeps_non_list_values_of_any_type(self):
        self.assertEqual(m.flatten(["a", ["b", 1]]), ["a", "b", 1])


if __name__ == "__main__":
    unittest.main()
