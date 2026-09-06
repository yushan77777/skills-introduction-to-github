import unittest

from tests.support import load

m = load("level1_basics")


class Task01Greet(unittest.TestCase):
    """Task 1: greet()"""

    def test_greets_by_name(self):
        self.assertEqual(m.greet("Ada"), "Hello, Ada!")

    def test_uses_the_name_it_is_given(self):
        self.assertEqual(m.greet("Grace"), "Hello, Grace!")

    def test_handles_an_empty_name(self):
        self.assertEqual(m.greet(""), "Hello, !")


class Task02CelsiusToFahrenheit(unittest.TestCase):
    """Task 2: celsius_to_fahrenheit()"""

    def test_freezing_point(self):
        self.assertAlmostEqual(m.celsius_to_fahrenheit(0), 32.0)

    def test_boiling_point(self):
        self.assertAlmostEqual(m.celsius_to_fahrenheit(100), 212.0)

    def test_minus_forty_is_the_same_in_both_scales(self):
        self.assertAlmostEqual(m.celsius_to_fahrenheit(-40), -40.0)

    def test_handles_fractions(self):
        self.assertAlmostEqual(m.celsius_to_fahrenheit(37), 98.6)


class Task03FizzBuzz(unittest.TestCase):
    """Task 3: fizzbuzz()"""

    def test_first_five(self):
        self.assertEqual(m.fizzbuzz(5), ["1", "2", "Fizz", "4", "Buzz"])

    def test_fifteen_is_fizzbuzz(self):
        self.assertEqual(m.fizzbuzz(15)[14], "FizzBuzz")

    def test_returns_strings_not_numbers(self):
        self.assertTrue(all(isinstance(item, str) for item in m.fizzbuzz(20)))

    def test_zero_gives_an_empty_list(self):
        self.assertEqual(m.fizzbuzz(0), [])


if __name__ == "__main__":
    unittest.main()
