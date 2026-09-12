"""
Password decryption for the ATM E-Journal ETL.

The project already ships an encryptor/decryptor that stores its key in a pickle
file and exposes ``get_decrypt_data(<token>)`` (as used by the existing Oracle
job: ``"password": enc.get_decrypt_data(oc["password"])``). This module *reuses*
that implementation - it imports the encryptor module named in the configuration
(``encryption.MODULE``) from ``src/`` and calls the configured method.

Only when that module is absent and ``encryption.ALLOW_BUILTIN_FALLBACK`` is
true does it fall back to the equivalent built-in implementation, which reads
the same pickle key store and the same Fernet token format - not a second,
unrelated mechanism.

Rules enforced here
-------------------
* a decrypted value is never logged, never put into an exception message and
  never written to the run summary;
* failures raise :class:`DecryptionError` carrying the *key name* only.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import pickle
from typing import Any, Callable, List, Optional

logger = logging.getLogger("atm_ejournal.encryption")


class DecryptionError(Exception):
    """Raised when a configured secret cannot be decrypted."""


def mask(value: Optional[str]) -> str:
    """Render a secret for a log line. Never returns any part of the secret."""
    return "********" if value else "<empty>"


# --------------------------------------------------------------------------- #
# Built-in fallback (same key store + token format as the project encryptor)
# --------------------------------------------------------------------------- #


class BuiltinFernetEncryptor:
    """
    Minimal stand-in for the project encryptor.

    Reads the Fernet key from ``pickle_path`` - accepting the shapes the project
    pickles are written in (raw ``bytes``/``str`` key, ``{"key": ...}`` mapping,
    or a one element list/tuple) - and exposes the same method names.
    """

    def __init__(self, pickle_path: str):
        self.pickle_path = pickle_path
        self._fernet = None

    def _load(self):
        if self._fernet is not None:
            return self._fernet
        if not os.path.isfile(self.pickle_path):
            raise DecryptionError(f"encryption key file not found: {self.pickle_path}")
        try:
            with open(self.pickle_path, "rb") as handle:
                payload = pickle.load(handle)
        except Exception as exc:                       # noqa: BLE001 - reported, not leaked
            raise DecryptionError(
                f"encryption key file could not be read ({self.pickle_path}): "
                f"{type(exc).__name__}") from None

        key = payload
        if isinstance(payload, dict):
            for candidate in ("key", "KEY", "fernet_key", "secret"):
                if candidate in payload:
                    key = payload[candidate]
                    break
            else:
                raise DecryptionError(f"no key entry found in {self.pickle_path}")
        elif isinstance(payload, (list, tuple)) and payload:
            key = payload[0]
        if isinstance(key, str):
            key = key.encode()
        if not isinstance(key, (bytes, bytearray)):
            raise DecryptionError(f"unsupported key type in {self.pickle_path}")

        try:
            from cryptography.fernet import Fernet
        except ImportError as exc:                     # pragma: no cover
            raise DecryptionError("cryptography is required for the built-in "
                                  "encryptor fallback") from exc
        try:
            self._fernet = Fernet(key)
        except Exception as exc:                       # noqa: BLE001
            raise DecryptionError(f"invalid encryption key in {self.pickle_path}: "
                                  f"{type(exc).__name__}") from None
        return self._fernet

    def get_decrypt_data(self, token: str) -> str:
        fernet = self._load()
        try:
            return fernet.decrypt(str(token).encode()).decode()
        except Exception as exc:                       # noqa: BLE001 - token never echoed
            raise DecryptionError("value could not be decrypted with the configured key "
                                  f"({type(exc).__name__})") from None

    def get_encrypt_data(self, value: str) -> str:
        return self._load().encrypt(str(value).encode()).decode()

    def __repr__(self) -> str:                         # pragma: no cover
        return f"<BuiltinFernetEncryptor {self.pickle_path}>"


# --------------------------------------------------------------------------- #
# Resolver
# --------------------------------------------------------------------------- #


class SecretResolver:
    """
    Decrypts configured secrets using the project encryptor.

    One instance per run; decryptors are cached per pickle file so the key store
    is read once. Instances are safe to keep on the driver - they hold the
    encryptor, not decrypted values.
    """

    def __init__(self,
                 modules: Optional[List[str]] = None,
                 factory: str = "Encryptor",
                 decrypt_method: str = "get_decrypt_data",
                 encrypt_method: str = "get_encrypt_data",
                 default_pickle: Optional[str] = None,
                 allow_builtin_fallback: bool = True,
                 base_dir: Optional[str] = None):
        self.modules = modules or ["encryptor"]
        self.factory = factory
        self.decrypt_method = decrypt_method
        self.encrypt_method = encrypt_method
        self.default_pickle = default_pickle
        self.allow_builtin_fallback = allow_builtin_fallback
        self.base_dir = base_dir or os.getcwd()
        self._cache: dict = {}
        self.implementation: Optional[str] = None

    # -- construction ------------------------------------------------------ #

    @classmethod
    def from_config(cls, cfg) -> "SecretResolver":
        """Build a resolver from the ``encryption:`` section of the config."""
        return cls(
            modules=cfg.get_list("encryption.MODULE", ["encryptor"]),
            factory=str(cfg.get("encryption.FACTORY", "Encryptor")),
            decrypt_method=str(cfg.get("encryption.DECRYPT_METHOD", "get_decrypt_data")),
            encrypt_method=str(cfg.get("encryption.ENCRYPT_METHOD", "get_encrypt_data")),
            default_pickle=cfg.path("encryption.PICKLE_PATH",
                                    "config/pickle/encryption.pkl"),
            allow_builtin_fallback=cfg.get_bool("encryption.ALLOW_BUILTIN_FALLBACK", True),
            base_dir=cfg.base_dir,
        )

    # -- internals --------------------------------------------------------- #

    def _resolve_pickle(self, pickle_path: Optional[str]) -> str:
        path = pickle_path or self.default_pickle
        if not path:
            raise DecryptionError("no encryption pickle configured")
        path = os.path.expanduser(str(path))
        return path if os.path.isabs(path) else os.path.normpath(os.path.join(self.base_dir, path))

    def _build_project_encryptor(self, pickle_path: str) -> Optional[Any]:
        """Instantiate the encryptor shipped with the project, when present."""
        for module_name in self.modules:
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                continue
            except Exception as exc:                   # noqa: BLE001 - broken module
                logger.warning("encryptor module '%s' failed to import: %s",
                               module_name, type(exc).__name__)
                continue

            factory = getattr(module, self.factory, None)
            if factory is not None:
                instance = self._instantiate(factory, pickle_path)
                if instance is not None and hasattr(instance, self.decrypt_method):
                    self.implementation = f"{module_name}.{self.factory}"
                    return instance

            function = getattr(module, self.decrypt_method, None)
            if callable(function):
                self.implementation = f"{module_name}.{self.decrypt_method}"
                return _FunctionEncryptor(function, pickle_path, self.decrypt_method,
                                          getattr(module, self.encrypt_method, None))
            logger.debug("module '%s' has neither %s nor %s",
                         module_name, self.factory, self.decrypt_method)
        return None

    @staticmethod
    def _instantiate(factory: Callable, pickle_path: str) -> Optional[Any]:
        """Call the encryptor factory with whatever signature it exposes."""
        attempts = ((pickle_path,), ())
        for args in attempts:
            try:
                instance = factory(*args)
            except TypeError:
                continue
            except Exception as exc:                   # noqa: BLE001
                logger.warning("encryptor factory failed: %s", type(exc).__name__)
                return None
            if not args:
                for attribute in ("pickle_path", "pickle", "key_file", "path"):
                    if hasattr(instance, attribute):
                        try:
                            setattr(instance, attribute, pickle_path)
                        except Exception:              # noqa: BLE001 - read-only attribute
                            pass
                        break
            return instance
        return None

    def _decryptor(self, pickle_path: Optional[str]) -> Any:
        path = self._resolve_pickle(pickle_path)
        if path in self._cache:
            return self._cache[path]

        instance = self._build_project_encryptor(path)
        if instance is None:
            if not self.allow_builtin_fallback:
                raise DecryptionError(
                    "no encryptor module found (tried: " + ", ".join(self.modules) +
                    ") and encryption.ALLOW_BUILTIN_FALLBACK is disabled")
            logger.warning("project encryptor module not found (tried: %s) - using the "
                           "built-in Fernet/pickle fallback", ", ".join(self.modules))
            instance = BuiltinFernetEncryptor(path)
            self.implementation = "builtin.BuiltinFernetEncryptor"

        logger.info("secret decryption uses %s with key store %s",
                    self.implementation, os.path.basename(path))
        self._cache[path] = instance
        return instance

    # -- public API -------------------------------------------------------- #

    def decrypt(self, ciphertext: Optional[str], pickle_path: Optional[str] = None,
                key_name: str = "value") -> str:
        """
        Decrypt one configured value. An empty value decrypts to an empty string
        so that "no password" stays a valid configuration.
        """
        if ciphertext is None or str(ciphertext).strip() == "":
            return ""
        decryptor = self._decryptor(pickle_path)
        method = getattr(decryptor, self.decrypt_method, None)
        if not callable(method):
            raise DecryptionError(f"encryptor has no callable {self.decrypt_method}()")
        try:
            plain = method(str(ciphertext))
        except DecryptionError:
            raise
        except Exception as exc:                       # noqa: BLE001 - never echo the value
            raise DecryptionError(f"could not decrypt configuration value '{key_name}' "
                                  f"({type(exc).__name__})") from None
        if isinstance(plain, (bytes, bytearray)):
            plain = plain.decode("utf-8", errors="replace")
        if plain is None:
            raise DecryptionError(f"decryption of '{key_name}' returned nothing")
        return str(plain)

    def encrypt(self, plaintext: str, pickle_path: Optional[str] = None) -> str:
        """Encrypt a value with the same mechanism (used by the CLI helper)."""
        decryptor = self._decryptor(pickle_path)
        method = getattr(decryptor, self.encrypt_method, None)
        if not callable(method):
            raise DecryptionError(f"encryptor has no callable {self.encrypt_method}()")
        return str(method(plaintext))

    def resolve_config_secret(self, cfg, value_key: str, pickle_key: Optional[str] = None) -> str:
        """Decrypt a secret addressed by its configuration keys."""
        ciphertext = cfg.get(value_key)
        pickle_path = None
        if pickle_key:
            configured = cfg.get(pickle_key)
            if configured:
                pickle_path = cfg.path(pickle_key)
        return self.decrypt(ciphertext, pickle_path, key_name=value_key)


class _FunctionEncryptor:
    """Adapter for an encryptor module that exposes plain functions."""

    def __init__(self, function: Callable, pickle_path: str, method_name: str,
                 encrypt_function: Optional[Callable] = None):
        self._function = function
        self._encrypt_function = encrypt_function
        self._pickle_path = pickle_path
        self._accepts_pickle = self._takes_two_arguments(function)
        setattr(self, method_name, self._call)

    @staticmethod
    def _takes_two_arguments(function: Callable) -> bool:
        try:
            return len(inspect.signature(function).parameters) >= 2
        except (TypeError, ValueError):                # pragma: no cover - builtins
            return False

    def _call(self, token: str) -> str:
        if self._accepts_pickle:
            return self._function(token, self._pickle_path)
        return self._function(token)

    def get_encrypt_data(self, value: str) -> str:
        if self._encrypt_function is None:
            raise DecryptionError("the encryptor module exposes no encrypt function")
        if self._takes_two_arguments(self._encrypt_function):
            return self._encrypt_function(value, self._pickle_path)
        return self._encrypt_function(value)


# --------------------------------------------------------------------------- #
# CLI helper: encrypt a value for the configuration file
# --------------------------------------------------------------------------- #

if __name__ == "__main__":             # pragma: no cover - operator utility
    import argparse
    import getpass
    import sys

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config_loader import load_config       # noqa: E402

    arguments = argparse.ArgumentParser(
        description="Encrypt a password for config/atm_ejournal.conf using the "
                    "project encryptor (the value is read from a prompt, never "
                    "from the command line).")
    arguments.add_argument("--config", default=None)
    arguments.add_argument("--etl", default=None)
    arguments.add_argument("--pickle", default=None, help="override encryption.PICKLE_PATH")
    arguments.add_argument("--init-key", action="store_true",
                           help="create a new Fernet key pickle when the project key store "
                                "does not exist yet (development / first setup only)")
    options = arguments.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    configuration = load_config(options.config, options.etl, validate=False)
    resolver = SecretResolver.from_config(configuration)
    key_store = options.pickle or resolver.default_pickle

    if options.init_key:
        if os.path.exists(key_store):
            print(f"key store already exists, not overwriting: {key_store}")
            sys.exit(1)
        from cryptography.fernet import Fernet

        os.makedirs(os.path.dirname(os.path.abspath(key_store)), exist_ok=True)
        with open(key_store, "wb") as handle:
            pickle.dump(Fernet.generate_key(), handle)
        os.chmod(key_store, 0o600)
        print(f"key store created: {key_store}")
        sys.exit(0)

    secret = getpass.getpass("value to encrypt: ")
    print(resolver.encrypt(secret, options.pickle))
