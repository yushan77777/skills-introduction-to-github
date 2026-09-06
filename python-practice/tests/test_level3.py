import unittest

from tests.support import load

m = load("level3_dicts_sets")


class Task07WordCount(unittest.TestCase):
    """Task 7: word_count()"""

    def test_counts_repeated_words(self):
        self.assertEqual(m.word_count("the cat the dog"), {"the": 2, "cat": 1, "dog": 1})

    def test_is_case_insensitive(self):
        self.assertEqual(m.word_count("Go go GO"), {"go": 3})

    def test_empty_text(self):
        self.assertEqual(m.word_count(""), {})


class Task08GroupByFirstLetter(unittest.TestCase):
    """Task 8: group_by_first_letter()"""

    def test_groups_words(self):
        self.assertEqual(
            m.group_by_first_letter(["apple", "avocado", "beet"]),
            {"a": ["apple", "avocado"], "b": ["beet"]},
        )

    def test_lowercases_the_key(self):
        self.assertEqual(m.group_by_first_letter(["Ant", "ape"]), {"a": ["Ant", "ape"]})

    def test_skips_empty_strings(self):
        self.assertEqual(m.group_by_first_letter(["", "bee"]), {"b": ["bee"]})

    def test_empty_input(self):
        self.assertEqual(m.group_by_first_letter([]), {})


class Task09Dedupe(unittest.TestCase):
    """Task 9: dedupe()"""

    def test_keeps_first_seen_order(self):
        self.assertEqual(m.dedupe([3, 1, 3, 2, 1]), [3, 1, 2])

    def test_works_with_strings(self):
        self.assertEqual(m.dedupe(["a", "b", "a"]), ["a", "b"])

    def test_leaves_unique_lists_alone(self):
        self.assertEqual(m.dedupe([1, 2, 3]), [1, 2, 3])

    def test_empty_list(self):
        self.assertEqual(m.dedupe([]), [])


if __name__ == "__main__":
    unittest.main()
