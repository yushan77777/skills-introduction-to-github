"""Level 1 - reference solutions."""


def greet(name):
    return f"Hello, {name}!"


def celsius_to_fahrenheit(celsius):
    return celsius * 9 / 5 + 32


def fizzbuzz(n):
    result = []
    for i in range(1, n + 1):
        if i % 15 == 0:
            result.append("FizzBuzz")
        elif i % 3 == 0:
            result.append("Fizz")
        elif i % 5 == 0:
            result.append("Buzz")
        else:
            result.append(str(i))
    return result
