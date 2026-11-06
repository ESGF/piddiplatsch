"""Disposable, checksummed fingerprints for unchanged closed dump files."""

import hashlib
import json
import tempfile
from pathlib import Path

_ROW_SIZE = 33  # Presence byte plus SHA-256; legacy records have no fingerprint.


def file_signature(path):
    stat = path.stat()
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


class DumpScanCache:
    def __init__(self, directory):
        self.directory = directory / ".deduplicate-cache"

    def _path(self, path):
        return self.directory / (
            hashlib.sha256(path.name.encode()).hexdigest() + ".bin"
        )

    def read(self, path, signature):
        """Validate the whole cache before returning any cached fingerprints."""
        cached = self._path(path)
        try:
            with cached.open("rb") as stream:
                header = stream.readline(4096)
                if json.loads(header) != {"version": 1, "signature": signature}:
                    return None
                size = cached.stat().st_size - len(header) - 32
                if size < 0 or size % _ROW_SIZE:
                    return None
                digest = hashlib.sha256(header)
                remaining = size
                while remaining:
                    block = stream.read(min(1024 * 1024, remaining))
                    if not block:
                        return None
                    digest.update(block)
                    remaining -= len(block)
                if stream.read(32) != digest.digest() or stream.read(1):
                    return None
        except (OSError, ValueError):
            return None

        def rows():
            with cached.open("rb") as stream:
                stream.seek(len(header))
                for _ in range(size // _ROW_SIZE):
                    row = stream.read(_ROW_SIZE)
                    yield row[1:] if row[0] else None

        return rows()

    def store(self, path, signature, rows):
        """Stream a cache to an atomic replacement; interrupted caches are discarded."""
        self.directory.mkdir(exist_ok=True)
        header = (json.dumps(dict(version=1, signature=signature)) + "\n").encode()
        digest = hashlib.sha256(header)
        with tempfile.TemporaryDirectory(
            dir=self.directory, prefix=".writing-"
        ) as scratch:
            temporary = Path(scratch) / "fingerprints"
            with temporary.open("wb") as stream:
                stream.write(header)
                for fingerprint in rows:
                    row = (
                        b"\x01" + fingerprint
                        if fingerprint is not None
                        else bytes(_ROW_SIZE)
                    )
                    stream.write(row)
                    digest.update(row)
                    yield fingerprint
                stream.write(digest.digest())
            temporary.replace(self._path(path))
