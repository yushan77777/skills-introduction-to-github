#!/usr/bin/env python3
"""Check your answers.

    python3 check.py          # every level
    python3 check.py 2        # just level 2
    python3 check.py 1 3      # levels 1 and 3
    python3 check.py --solutions   # sanity-check the reference answers

Nothing to install - this uses only the Python standard library.
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
LEVELS = {
    1: ("Level 1", "Basics: strings, numbers, loops"),
    2: ("Level 2", "Strings and lists"),
    3: ("Level 3", "Dictionaries and sets"),
    4: ("Level 4", "Logic and recursion"),
    5: ("Level 5", "Classes and error handling"),
}

GREEN, RED, YELLOW, DIM, BOLD, RESET = (
    ("\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[1m", "\033[0m")
    if sys.stdout.isatty()
    else ("", "", "", "", "", "")
)


class CollectingResult(unittest.TestResult):
    """Records the outcome of every individual test method."""

    def __init__(self):
        super().__init__()
        self.outcomes = []  # (test, status, message)

    def addSuccess(self, test):
        self.outcomes.append((test, "pass", ""))

    def addFailure(self, test, err):
        self.outcomes.append((test, "fail", self._short(err)))

    def addError(self, test, err):
        status = "todo" if err[0] is NotImplementedError else "error"
        self.outcomes.append((test, status, self._short(err)))

    @staticmethod
    def _short(err):
        exc_type, exc_value = err[0], err[1]
        text = str(exc_value).strip().splitlines()
        first = text[0] if text else ""
        if exc_type is AssertionError:
            return first
        return f"{exc_type.__name__}: {first}" if first else exc_type.__name__


def task_label(test):
    """Use the TestCase docstring ("Task 3: fizzbuzz()") as the task name."""
    doc = (test.__class__.__doc__ or test.__class__.__name__).strip()
    return doc.splitlines()[0]


def readable(test):
    return test._testMethodName[len("test_"):].replace("_", " ")


def run_level(level):
    module = f"tests.test_level{level}"
    suite = unittest.defaultTestLoader.loadTestsFromName(module)
    result = CollectingResult()
    suite.run(result)

    tasks = {}
    for test, status, message in result.outcomes:
        tasks.setdefault(task_label(test), []).append((test, status, message))

    name, blurb = LEVELS[level]
    print(f"\n{BOLD}{name}{RESET} {DIM}- {blurb}{RESET}")
    done = 0
    for label, outcomes in tasks.items():
        statuses = [status for _, status, _ in outcomes]
        if all(status == "pass" for status in statuses):
            done += 1
            print(f"  {GREEN}PASS{RESET}  {label}  {DIM}({len(outcomes)} checks){RESET}")
            continue
        if all(status == "todo" for status in statuses):
            print(f"  {YELLOW}TODO{RESET}  {label}  {DIM}not started yet{RESET}")
            continue
        passed = statuses.count("pass")
        print(f"  {RED}FAIL{RESET}  {label}  {DIM}({passed}/{len(outcomes)} checks){RESET}")
        for test, status, message in outcomes:
            if status != "pass":
                print(f"        - {readable(test)}")
                if message:
                    print(f"          {DIM}{message}{RESET}")
    return done, len(tasks)


def main(argv):
    args = [a for a in argv if a != "--solutions"]
    if "--solutions" in argv:
        os.environ["PYPRACTICE_PACKAGE"] = "solutions"
        print(f"{DIM}(checking the reference solutions){RESET}")

    try:
        levels = sorted({int(a) for a in args}) or sorted(LEVELS)
    except ValueError:
        print("Usage: python3 check.py [level ...] [--solutions]")
        return 2
    unknown = [level for level in levels if level not in LEVELS]
    if unknown:
        print(f"No such level: {unknown[0]}. Levels are 1-{max(LEVELS)}.")
        return 2

    sys.path.insert(0, HERE)
    done = total = 0
    for level in levels:
        level_done, level_total = run_level(level)
        done += level_done
        total += level_total

    bar_width = 20
    filled = round(bar_width * done / total) if total else 0
    bar = "#" * filled + "." * (bar_width - filled)
    print(f"\n{BOLD}{done}/{total} tasks complete{RESET}  [{bar}]")
    if done == total:
        print(f"{GREEN}All done - nice work.{RESET}")
    else:
        print(f"{DIM}Stuck? The reference answers live in solutions/.{RESET}")
    return 0 if done == total else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
