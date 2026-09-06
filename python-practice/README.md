# Python Practice

15 small Python tasks, in five levels, with tests that tell you the moment an
answer is right. No libraries to install — everything here uses only the Python
standard library.

## How it works

1. Open a file in `tasks/` and read the docstring above a function. It says what
   the function should return, shows examples, and gives a hint.
2. Replace the `raise NotImplementedError(...)` line with your own code.
3. Run the checker.

```bash
cd python-practice
python3 check.py        # check every level
python3 check.py 2      # check just level 2
python3 check.py 1 3    # check levels 1 and 3
```

The checker prints one line per task:

```
Level 1 - Basics: strings, numbers, loops
  PASS  Task 1: greet()  (3 checks)
  FAIL  Task 2: celsius_to_fahrenheit()  (2/4 checks)
        - boiling point
          212.0 != 211.99999
  TODO  Task 3: fizzbuzz()  not started yet
```

`TODO` means you haven't touched it yet, `FAIL` shows which specific check
disagreed with your code, and `PASS` means that task is finished.

## The tasks

| Level | File | Tasks |
| --- | --- | --- |
| 1 — Basics | `tasks/level1_basics.py` | `greet`, `celsius_to_fahrenheit`, `fizzbuzz` |
| 2 — Strings & lists | `tasks/level2_strings_lists.py` | `count_vowels`, `reverse_words`, `second_largest` |
| 3 — Dicts & sets | `tasks/level3_dicts_sets.py` | `word_count`, `group_by_first_letter`, `dedupe` |
| 4 — Logic & recursion | `tasks/level4_logic_recursion.py` | `is_palindrome`, `fib`, `flatten` |
| 5 — Classes | `tasks/level5_classes.py` | `BankAccount`, `Stack`, `safe_int` |

Work through them in order — each level leans on ideas from the one before it.

## If you get stuck

- Read the failure line the checker prints. `[3, 1, 2] != [3, 1, 2, 1]` usually
  points straight at the bug.
- Add `print(...)` inside your function and run the checker again; anything you
  print shows up in the output.
- Every answer is written out in `solutions/`. Try it yourself first, then
  compare — the interesting part is usually *why* the solution is shorter.

## Prefer to type in the browser?

The same 15 tasks are published as an interactive page with a built-in editor
that runs your Python in the browser and checks each answer as you go:

**https://claude.ai/code/artifact/77f2dff0-6ca0-46ba-a095-34d680feacc8**

Its source is `browser-console.html`. That file is the body of a Claude
Artifact, so it has no `<html>`/`<head>` wrapper of its own - the hosted page
above is the way to use it.

## Also useful

- `python3 check.py --solutions` runs the tests against `solutions/` instead of
  your own code, which confirms the test suite itself is healthy.
- The tests are plain `unittest` files, so `python3 -m unittest discover tests`
  works too if you prefer the standard output.

## Files

```
python-practice/
  check.py        the friendly test runner
  tasks/          <- you write your code here
  tests/          the checks each task must satisfy
  solutions/      reference answers
```
