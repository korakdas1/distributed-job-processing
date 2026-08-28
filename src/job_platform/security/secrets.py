"""Load API keys from files or environment values. Never log secret contents."""

from __future__ import annotations

from pathlib import Path

from pydantic import SecretStr

MAX_SECRET_FILE_BYTES = 4096


def parse_allowed_hosts(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def strip_one_trailing_newline(text: str) -> str:
    if text.endswith("\r\n"):
        return text[:-2]
    if text.endswith("\n"):
        return text[:-1]
    if text.endswith("\r"):
        return text[:-1]
    return text


def read_secret_file(path: str) -> str:
    file_path = Path(path)
    if not file_path.is_file():
        raise ValueError(f"secret file not found: {file_path.name}")
    data = file_path.read_bytes()
    if len(data) > MAX_SECRET_FILE_BYTES:
        raise ValueError("secret file exceeds maximum size")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("secret file is not valid UTF-8") from exc
    secret = strip_one_trailing_newline(text)
    if secret == "":
        raise ValueError("secret file is empty")
    if "\x00" in secret:
        raise ValueError("secret file contains a NUL byte")
    return secret


def load_api_key(
    *,
    raw: SecretStr | None,
    file_path: str | None,
    name: str,
    required: bool = False,
) -> str | None:
    """Resolve one API key.

    If both a secret file and a raw environment value are set, the file wins.
    """
    if file_path:
        value = read_secret_file(file_path)
        if value == "":
            raise ValueError(f"{name} API key is empty")
        return value
    if raw is not None:
        value = raw.get_secret_value()
        if value == "":
            raise ValueError(f"{name} API key is empty")
        return value
    if required:
        raise ValueError(
            f"{name} API key is required "
            f"(set {name.upper()}_API_KEY_FILE or {name.upper()}_API_KEY)"
        )
    return None
