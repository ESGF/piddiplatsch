"""Check local links after MkDocs and the standalone slides are assembled."""

from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class LinkChecker(HTMLParser):
    def __init__(self, page: Path):
        super().__init__()
        self.page = page
        self.missing = []

    def handle_starttag(self, tag, attrs):
        for key, value in attrs:
            if key not in {"href", "src"} or not value:
                continue
            url = urlsplit(value)
            if url.scheme or url.netloc or not url.path or url.path.startswith("/"):
                continue
            target = self.page.parent / unquote(url.path)
            if not target.exists():
                self.missing.append(f"{self.page}: {value}")


def main():
    root = Path(__file__).resolve().parents[1] / "site"
    required = ["index.html", "talks/index.html", "talks/overview.html"]
    errors = [f"Missing page: {root / name}" for name in required if not (root / name).is_file()]
    for page in root.rglob("*.html"):
        checker = LinkChecker(page)
        checker.feed(page.read_text(encoding="utf-8"))
        errors.extend(checker.missing)
    if errors:
        raise SystemExit("Broken documentation links/assets:\n" + "\n".join(errors))
    print("Documentation and slides: required pages and relative links/assets verified.")


if __name__ == "__main__":
    main()
