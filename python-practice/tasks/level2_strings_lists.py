"""Level 2 - Strings and lists."""


def count_vowels(text):
    """Count the vowels (a, e, i, o, u) in `text`, ignoring case.

    count_vowels("Hello World") -> 3
    count_vowels("") -> 0

    Hint: loop over the characters and test `ch.lower() in "aeiou"`.
    """
    raise NotImplementedError("Task 4: write count_vowels()")


def reverse_words(sentence):
    """Reverse the order of the words in a sentence.

    reverse_words("the quick brown fox") -> "fox brown quick the"

    Words are separated by whitespace; the result is joined by single spaces.

    Hint: `sentence.split()`, then a slice `[::-1]`, then `" ".join(...)`.
    """
    raise NotImplementedError("Task 5: write reverse_words()")


def second_largest(numbers):
    """Return the second largest *distinct* value in `numbers`.

    Return None when there are fewer than two distinct values.

    second_largest([3, 1, 4, 4, 5]) -> 4
    second_largest([2, 2, 2])       -> None
    second_largest([])              -> None

    Hint: `set()` removes duplicates, `sorted()` puts them in order.
    """
    raise NotImplementedError("Task 6: write second_largest()")
