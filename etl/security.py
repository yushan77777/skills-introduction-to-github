"""Keeping credentials out of logs, API responses and the execution history.

Two rules hold across the whole ETL app:

1. A submitted secret leaves the web process only on the ETL subprocess's
   stdin — never on a command line, never in a file, never in a response.
2. Every line captured from the ETL is filtered through :class:`Redactor`
   before anything can read it, including the on-disk run log.
"""

from __future__ import annotations

import re

MASK = "••••••"

#: Credentials embedded in connector output and JDBC URLs, e.g.
#: ``?user=x&password=secret`` or ``password=secret``.
_INLINE_SECRET = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key)\s*[=:]\s*"
    r"(\"[^\"]*\"|'[^']*'|\S+)"
)

#: ``jdbc:oracle:thin:user/password@//host:port/service`` — the supplied
#: config.yaml stores Oracle passwords in clear inside the URL itself.
_JDBC_USER_PW = re.compile(
    r"(?i)(jdbc:[a-z0-9]+:[a-z0-9]*:)([^/\s:@]+)/([^@\s/]+)@"
)

#: The generic ``scheme://user:password@host`` form.
_URL_USERINFO = re.compile(
    r"(?i)(\b[a-z][a-z0-9+.-]*://)([^:/\s@]+):([^@\s/]+)@"
)

CREDENTIAL_NAME = re.compile(r"(?i)(password|passwd|pwd|secret|token|api[_-]?key)")


class Redactor:
    """Masks known secret values plus anything that reads like a credential."""

    def __init__(self, secrets: list[str] | None = None):
        # Longest first, so a password containing another still masks fully.
        self._secrets = sorted(
            {s for s in (secrets or []) if isinstance(s, str) and len(s) >= 3},
            key=len,
            reverse=True,
        )

    def add(self, value: str | None) -> None:
        if isinstance(value, str) and len(value) >= 3:
            self._secrets = sorted(
                set(self._secrets) | {value}, key=len, reverse=True)

    def __call__(self, text: str) -> str:
        if not text:
            return text
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, MASK)
        text = _mask_urls(text)
        return _INLINE_SECRET.sub(lambda m: f"{m.group(1)}={MASK}", text)


def strip_sensitive(values: dict, sensitive: set[str]) -> dict:
    """Copy of a payload with sensitive keys removed, for echo-back and history.

    Anything whose *name* reads like a credential goes too, so a hand-crafted
    payload cannot park a password in the execution history.
    """
    return {
        k: v for k, v in values.items()
        if k not in sensitive and not CREDENTIAL_NAME.search(k)
    }


def _mask_urls(text: str) -> str:
    text = _JDBC_USER_PW.sub(lambda m: f"{m.group(1)}{m.group(2)}/{MASK}@", text)
    return _URL_USERINFO.sub(lambda m: f"{m.group(1)}{m.group(2)}:{MASK}@", text)


def mask_url_credentials(url: str) -> str:
    """``jdbc:oracle:thin:user/pw@//host/svc`` -> the same with ``pw`` masked."""
    return _mask_urls(url) if url else url
