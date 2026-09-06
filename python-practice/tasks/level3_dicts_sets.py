"""Level 3 - Dictionaries and sets."""


def word_count(text):
    """Count how often each word appears, ignoring case.

    Split on whitespace only (punctuation stays attached to the word).

    word_count("the cat the dog") -> {"the": 2, "cat": 1, "dog": 1}
    word_count("") -> {}

    Hint: `counts.get(word, 0) + 1` avoids a KeyError on the first sighting.
    """
    raise NotImplementedError("Task 7: write word_count()")


def group_by_first_letter(words):
    """Group words by their (lowercased) first letter.

    Words keep their original order inside each group, and empty strings
    are skipped.

    group_by_first_letter(["apple", "avocado", "beet"])
        -> {"a": ["apple", "avocado"], "b": ["beet"]}

    Hint: `groups.setdefault(letter, []).append(word)`.
    """
    raise NotImplementedError("Task 8: write group_by_first_letter()")


def dedupe(items):
    """Remove duplicates from a list while keeping the first-seen order.

    dedupe([3, 1, 3, 2, 1]) -> [3, 1, 2]

    Hint: track what you have already seen in a `set`, because checking
    membership in a set is far faster than scanning a list.
    """
    raise NotImplementedError("Task 9: write dedupe()")
