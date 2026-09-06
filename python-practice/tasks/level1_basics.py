"""Level 1 - Basics: strings, numbers, loops.

Replace each `raise NotImplementedError` with your own code, then run:

    python3 check.py 1
"""


def greet(name):
    """Return a greeting.

    greet("Ada") -> "Hello, Ada!"

    Hint: f-strings make this a one-liner: f"Hello, {name}!"
    """
    raise NotImplementedError("Task 1: write greet()")


def celsius_to_fahrenheit(celsius):
    """Convert a Celsius temperature to Fahrenheit.

    Formula: F = C * 9 / 5 + 32

    celsius_to_fahrenheit(0)   -> 32.0
    celsius_to_fahrenheit(100) -> 212.0
    celsius_to_fahrenheit(-40) -> -40.0
    """
    raise NotImplementedError("Task 2: write celsius_to_fahrenheit()")


def fizzbuzz(n):
    """Return the FizzBuzz list for the numbers 1 through n.

    For each number: "Fizz" if divisible by 3, "Buzz" if divisible by 5,
    "FizzBuzz" if divisible by both, otherwise the number as a string.

    fizzbuzz(5) -> ["1", "2", "Fizz", "4", "Buzz"]

    Hint: build an empty list, loop with `for i in range(1, n + 1)`,
    and check `i % 15 == 0` first.
    """
    raise NotImplementedError("Task 3: write fizzbuzz()")
