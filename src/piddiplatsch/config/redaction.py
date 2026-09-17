"""Non-mutating redaction for human-readable configuration output."""

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED = "***"


def _sensitive_key(key: str) -> bool:
    words = re.sub(r"([a-z])([A-Z])", r"\1_\2", key).lower()
    parts = set(re.split(r"[._\-\s]+", words))
    return bool(
        parts
        & {
            "password",
            "passwd",
            "passphrase",
            "secret",
            "token",
            "credentials",
            "authorization",
            "username",
        }
        or {"api", "key"} <= parts
        or {"access", "key"} <= parts
        or {"private", "key"} <= parts
        or words
        in {
            "apikey",
            "accesskey",
            "privatekey",
            "clientsecret",
            "sasl.jaas.config",
            "sasl.oauthbearer.config",
            "ssl.key.pem",
        }
    )


def _redact_url(value: str) -> str:
    if "://" not in value:
        return value
    try:
        url = urlsplit(value)
        netloc = url.netloc
        if "@" in netloc:
            netloc = f"{REDACTED}@{netloc.rsplit('@', 1)[1]}"
        query = parse_qsl(url.query, keep_blank_values=True)
        sensitive_query = any(_sensitive_key(key) for key, _ in query)
        if netloc == url.netloc and not sensitive_query:
            return value
        return urlunsplit(
            (
                url.scheme,
                netloc,
                url.path,
                (
                    urlencode(
                        [
                            (key, REDACTED if _sensitive_key(key) else item)
                            for key, item in query
                        ]
                    )
                    if sensitive_query
                    else url.query
                ),
                url.fragment,
            )
        )
    except ValueError:
        # Do not print an unparseable URL that could contain embedded credentials.
        return REDACTED


def redact_config(value):
    """Mask known credential keys and URL credentials in a fresh structure."""
    if isinstance(value, dict):
        return {
            key: REDACTED if _sensitive_key(key) else redact_config(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_config(item) for item in value]
    if isinstance(value, str):
        return _redact_url(value)
    return value
