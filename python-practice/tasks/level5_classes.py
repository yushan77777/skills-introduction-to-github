"""Level 5 - Classes and error handling."""


class BankAccount:
    """A tiny bank account.

    account = BankAccount("Ada", 100)
    account.owner     -> "Ada"
    account.balance   -> 100
    account.deposit(50)    # balance becomes 150
    account.withdraw(20)   # balance becomes 130

    Rules:
      * `balance` defaults to 0 when not given.
      * deposit() and withdraw() raise ValueError when the amount is <= 0.
      * withdraw() raises ValueError when the amount is more than the balance.

    Hint: `raise ValueError("Insufficient funds")` stops the method there.
    """

    def __init__(self, owner, balance=0):
        raise NotImplementedError("Task 13: write BankAccount.__init__()")

    def deposit(self, amount):
        raise NotImplementedError("Task 13: write BankAccount.deposit()")

    def withdraw(self, amount):
        raise NotImplementedError("Task 13: write BankAccount.withdraw()")


class Stack:
    """A last-in, first-out stack.

    stack = Stack()
    stack.push(1); stack.push(2)
    len(stack)     -> 2
    stack.peek()   -> 2      (look without removing)
    stack.pop()    -> 2
    stack.is_empty() -> False

    pop() and peek() on an empty stack raise IndexError.

    Hint: store the items in a plain list; `__len__` makes len() work.
    """

    def __init__(self):
        raise NotImplementedError("Task 14: write Stack.__init__()")

    def push(self, item):
        raise NotImplementedError("Task 14: write Stack.push()")

    def pop(self):
        raise NotImplementedError("Task 14: write Stack.pop()")

    def peek(self):
        raise NotImplementedError("Task 14: write Stack.peek()")

    def is_empty(self):
        raise NotImplementedError("Task 14: write Stack.is_empty()")

    def __len__(self):
        raise NotImplementedError("Task 14: write Stack.__len__()")


def safe_int(value, default=0):
    """Convert `value` to an int, returning `default` when that is impossible.

    safe_int("42")    -> 42
    safe_int("abc")   -> 0
    safe_int(None, -1) -> -1
    safe_int("7.9")   -> 0      (int("7.9") raises ValueError)

    Hint: wrap `int(value)` in try/except and catch (ValueError, TypeError).
    """
    raise NotImplementedError("Task 15: write safe_int()")
