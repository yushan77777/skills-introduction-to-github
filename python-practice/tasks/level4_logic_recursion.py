"""Level 4 - Logic and recursion."""


def is_palindrome(text):
    """Return True if `text` reads the same backwards.

    Ignore case, spaces, and punctuation - compare letters and digits only.

    is_palindrome("A man, a plan, a canal: Panama") -> True
    is_palindrome("hello")                          -> False
    is_palindrome("")                               -> True

    Hint: `ch.isalnum()` tells you whether to keep a character.
    """
    raise NotImplementedError("Task 10: write is_palindrome()")


def fib(n):
    """Return the nth Fibonacci number, where fib(0) == 0 and fib(1) == 1.

    fib(2) -> 1, fib(7) -> 13, fib(30) -> 832040

    Hint: a loop that keeps two running values (a, b = b, a + b) stays fast
    even for fib(30); plain recursion without caching gets very slow.
    """
    raise NotImplementedError("Task 11: write fib()")


def flatten(nested):
    """Flatten an arbitrarily nested list into a single flat list.

    flatten([1, [2, [3, [4]], 5]]) -> [1, 2, 3, 4, 5]
    flatten([]) -> []

    Hint: loop over the items; if an item `isinstance(item, list)`, call
    flatten() on it and extend the result, otherwise append the item.
    """
    raise NotImplementedError("Task 12: write flatten()")
