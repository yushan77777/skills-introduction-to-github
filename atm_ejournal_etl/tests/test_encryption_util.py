"""Password decryption: reuse of the project encryptor, and the fallback."""

from __future__ import annotations

import logging
import os
import pickle
import sys

import pytest

from encryption_util import (BuiltinFernetEncryptor, DecryptionError, SecretResolver,
                             mask)


def _write_key_pickle(path: str):
    from cryptography.fernet import Fernet

    key = Fernet.generate_key()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handle:
        pickle.dump(key, handle)
    return key


# --------------------------------------------------------------------------- #
# Built-in fallback (same pickle + Fernet token format as the project encryptor)
# --------------------------------------------------------------------------- #


def test_builtin_round_trip(tmp_path):
    pickle_path = str(tmp_path / "pickle" / "encryption.pkl")
    _write_key_pickle(pickle_path)
    encryptor = BuiltinFernetEncryptor(pickle_path)
    token = encryptor.get_encrypt_data("s3cret-password")
    assert token != "s3cret-password"
    assert encryptor.get_decrypt_data(token) == "s3cret-password"


def test_key_stored_as_dict_is_accepted(tmp_path):
    from cryptography.fernet import Fernet

    pickle_path = tmp_path / "encryption.pkl"
    key = Fernet.generate_key()
    pickle_path.write_bytes(pickle.dumps({"key": key}))
    encryptor = BuiltinFernetEncryptor(str(pickle_path))
    assert encryptor.get_decrypt_data(Fernet(key).encrypt(b"abc").decode()) == "abc"


def test_missing_pickle_file(tmp_path):
    encryptor = BuiltinFernetEncryptor(str(tmp_path / "nope.pkl"))
    with pytest.raises(DecryptionError, match="key file not found"):
        encryptor.get_decrypt_data("gAAAAA-token")


def test_invalid_token_does_not_leak_the_value(tmp_path):
    pickle_path = str(tmp_path / "encryption.pkl")
    _write_key_pickle(pickle_path)
    encryptor = BuiltinFernetEncryptor(pickle_path)
    with pytest.raises(DecryptionError) as error:
        encryptor.get_decrypt_data("not-a-valid-token")
    assert "not-a-valid-token" not in str(error.value)


def test_invalid_key_in_pickle(tmp_path):
    pickle_path = tmp_path / "encryption.pkl"
    pickle_path.write_bytes(pickle.dumps(b"not-a-fernet-key"))
    with pytest.raises(DecryptionError, match="invalid encryption key"):
        BuiltinFernetEncryptor(str(pickle_path)).get_decrypt_data("x")


# --------------------------------------------------------------------------- #
# Reuse of the encryptor shipped with the project
# --------------------------------------------------------------------------- #


PROJECT_ENCRYPTOR = '''
class Encryptor:
    """Stand-in for the encryptor file dropped into src/."""

    def __init__(self, pickle_path=None):
        self.pickle_path = pickle_path

    def get_decrypt_data(self, token):
        return "decrypted:" + str(token)

    def get_encrypt_data(self, value):
        return "encrypted:" + str(value)
'''

FUNCTION_ENCRYPTOR = '''
def get_decrypt_data(token):
    return "fn:" + str(token)
'''


@pytest.fixture
def encryptor_module(tmp_path, monkeypatch):
    """Write an encryptor module and make it importable, like src/encryptor.py."""
    def _write(name: str, body: str):
        path = tmp_path / f"{name}.py"
        path.write_text(body)
        monkeypatch.syspath_prepend(str(tmp_path))
        sys.modules.pop(name, None)
        return name
    yield _write
    for module in list(sys.modules):
        if module.startswith("project_encryptor"):
            sys.modules.pop(module, None)


def test_project_encryptor_class_is_reused(encryptor_module, tmp_path):
    name = encryptor_module("project_encryptor_a", PROJECT_ENCRYPTOR)
    resolver = SecretResolver(modules=[name], default_pickle=str(tmp_path / "p.pkl"))
    assert resolver.decrypt("TOKEN") == "decrypted:TOKEN"
    assert resolver.implementation == f"{name}.Encryptor"
    assert resolver.encrypt("plain") == "encrypted:plain"


def test_project_encryptor_function_is_reused(encryptor_module, tmp_path):
    name = encryptor_module("project_encryptor_b", FUNCTION_ENCRYPTOR)
    resolver = SecretResolver(modules=[name], default_pickle=str(tmp_path / "p.pkl"))
    assert resolver.decrypt("TOKEN") == "fn:TOKEN"


def test_falls_back_when_no_encryptor_module(tmp_path):
    pickle_path = str(tmp_path / "encryption.pkl")
    _write_key_pickle(pickle_path)
    resolver = SecretResolver(modules=["definitely_not_installed"], default_pickle=pickle_path)
    token = resolver.encrypt("pw")
    assert resolver.decrypt(token) == "pw"
    assert resolver.implementation == "builtin.BuiltinFernetEncryptor"


def test_fallback_can_be_disabled(tmp_path):
    resolver = SecretResolver(modules=["definitely_not_installed"],
                              default_pickle=str(tmp_path / "p.pkl"),
                              allow_builtin_fallback=False)
    with pytest.raises(DecryptionError, match="ALLOW_BUILTIN_FALLBACK"):
        resolver.decrypt("token")


def test_empty_value_decrypts_to_empty_string(tmp_path):
    resolver = SecretResolver(modules=["nope"], default_pickle=str(tmp_path / "p.pkl"))
    assert resolver.decrypt("") == ""
    assert resolver.decrypt(None) == ""


def test_resolve_config_secret_uses_configured_pickle(cfg, etl_home):
    pickle_path = os.path.join(etl_home, "config", "pickle", "encryption.pkl")
    _write_key_pickle(pickle_path)
    resolver = SecretResolver.from_config(cfg)
    token = resolver.encrypt("greenplum-password")
    cfg._data["greenplum"]["GREENPLUM_PASSWORD"] = token       # noqa: SLF001
    assert resolver.resolve_config_secret(
        cfg, "greenplum.GREENPLUM_PASSWORD", "greenplum.GREENPLUM_PICKLE") == "greenplum-password"


def test_nothing_secret_is_logged(tmp_path, caplog):
    pickle_path = str(tmp_path / "encryption.pkl")
    _write_key_pickle(pickle_path)
    resolver = SecretResolver(modules=["nope"], default_pickle=pickle_path)
    token = resolver.encrypt("top-secret-value")
    with caplog.at_level(logging.DEBUG):
        assert resolver.decrypt(token, key_name="greenplum.GREENPLUM_PASSWORD") == "top-secret-value"
    assert "top-secret-value" not in caplog.text
    assert token not in caplog.text


def test_mask():
    assert mask("anything") == "********"
    assert mask("") == "<empty>"
