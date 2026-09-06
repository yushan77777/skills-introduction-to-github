"""Level 3 - reference solutions."""


def word_count(text):
    counts = {}
    for word in text.lower().split():
        counts[word] = counts.get(word, 0) + 1
    return counts


def group_by_first_letter(words):
    groups = {}
    for word in words:
        if not word:
            continue
        groups.setdefault(word[0].lower(), []).append(word)
    return groups


def dedupe(items):
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result
