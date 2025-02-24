import random
import pkg_resources
from typing import List

_adjectives: List[str] | None = None
_nouns: List[str] | None = None


def _load_words(filename: str) -> List[str]:
    path = pkg_resources.resource_filename("syft_rds", f"assets/{filename}")
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def generate_name() -> str:
    """Generate a Docker-like name using random adjective and noun combinations."""
    global _adjectives, _nouns
    if _adjectives is None:
        _adjectives = _load_words("adjectives.txt")
    if _nouns is None:
        _nouns = _load_words("nouns.txt")
    return f"{random.choice(_adjectives)}_{random.choice(_nouns)}"
