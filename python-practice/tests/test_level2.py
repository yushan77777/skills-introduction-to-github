import unittest

from tests.support import load

m = load("level2_strings_lists")


class Task04CountVowels(unittest.TestCase):
    """Task 4: count_vowels()"""

    def test_counts_lowercase_vowels(self):
        self.assertEqual(m.count_vowels("hello world"), 3)

    def test_ignores_case(self):
        self.assertEqual(m.count_vowels("AEIOU"), 5)

    def test_no_vowels(self):
        self.assertEqual(m.count_vowels("rhythm"), 0)

    def test_empty_string(self):
        self.assertEqual(m.count_vowels(""), 0)


class Task05ReverseWords(unittest.TestCase):
    """Task 5: reverse_words()"""

    def test_reverses_word_order(self):
        self.assertEqual(m.reverse_words("the quick brown fox"), "fox brown quick the")

    def test_single_word_is_unchanged(self):
        self.assertEqual(m.reverse_words("python"), "python")

    def test_collapses_extra_whitespace(self):
        self.assertEqual(m.reverse_words("  a   b  "), "b a")

    def test_empty_string(self):
        self.assertEqual(m.reverse_words(""), "")


class Task06SecondLargest(unittest.TestCase):
    """Task 6: second_largest()"""

    def test_finds_second_largest(self):
        self.assertEqual(m.second_largest([3, 1, 4, 1, 5]), 4)

    def test_ignores_duplicates_of_the_maximum(self):
        self.assertEqual(m.second_largest([5, 5, 3]), 3)

    def test_all_equal_returns_none(self):
        self.assertIsNone(m.second_largest([2, 2, 2]))

    def test_empty_list_returns_none(self):
        self.assertIsNone(m.second_largest([]))

    def test_handles_negative_numbers(self):
        self.assertEqual(m.second_largest([-1, -2, -3]), -2)


if __name__ == "__main__":
    unittest.main()
