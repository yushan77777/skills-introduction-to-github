"""Level 2 - reference solutions."""


def count_vowels(text):
    return sum(1 for ch in text if ch.lower() in "aeiou")


def reverse_words(sentence):
    return " ".join(sentence.split()[::-1])


def second_largest(numbers):
    distinct = sorted(set(numbers))
    if len(distinct) < 2:
        return None
    return distinct[-2]
