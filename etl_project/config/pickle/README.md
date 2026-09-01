# `config/pickle/`

## What lives here

`pass1.pkl` — the **Fernet encryption key** used by
`src/utils/encrypt_module.Encrypter` to decrypt the `password` values in
`config/config.yaml`.

Despite the `.pkl` extension it is **not a pickle**. `Encrypter._load_fernet`
does:

```python
with open(self.key_path, 'rb') as f:
    key = f.read()
return Fernet(key)
```

so the file is 44 bytes of raw URL-safe base64 — exactly what
`Fernet.generate_key()` writes, with no trailing newline. Nothing is
unpickled, and no arbitrary-code-execution risk comes with it.

## Why it is not in version control

It is a credential. `.gitignore` excludes `config/pickle/*.pkl` for the same
reason it excludes `config/config.yaml` and the Monitoring app's `config.ini`.

## Restoring it

The key is already on the ETL server. Copy it from there:

```bash
mkdir -p config/pickle
cp /path/to/existing/etl/config/pickle/pass1.pkl config/pickle/pass1.pkl
chmod 600 config/pickle/pass1.pkl
```

Verify it matches the encrypted passwords in `config.yaml` — the ETL
Configuration panel in the web UI reports this per connection profile, or from
a shell:

```bash
python - <<'PY'
import yaml
from src.utils.encrypt_module import Encrypter
cfg = yaml.safe_load(open("config/config.yaml"))
for name, block in cfg.items():
    if not isinstance(block, dict) or "pickle" not in block:
        continue
    try:
        Encrypter(block["pickle"]).get_decrypt_data(block["password"])
        print(f"{name}: OK")
    except Exception as exc:
        print(f"{name}: FAILED ({exc.__class__.__name__})")
PY
```

## Generating a new key (only when rotating)

```bash
python -c "from src.utils.encrypt_module import Encrypter; \
           Encrypter('config/pickle/pass1.pkl').generate_key()"
```

A new key invalidates **every** encrypted password in `config.yaml`. Re-encrypt
each one with `python src/utils/encrypt_module.py '<plaintext>'` and paste the
new ciphertexts back into the file.
