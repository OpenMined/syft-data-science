import random
import string
from typing import List

from syft_rds.utils.resources import load_resource

_adjectives: List[str] | None = None
_nouns: List[str] | None = None


def _load_words(filename: str) -> List[str]:
    content = load_resource(filename)
    return [line.strip() for line in content.splitlines() if line.strip()]


def generate_name() -> str:
    """Generate a Docker-like name using random adjective and noun combinations,
    followed by 4 random alphanumeric characters."""
    global _adjectives, _nouns
    if _adjectives is None:
        _adjectives = _load_words("adjectives.txt")
    if _nouns is None:
        _nouns = _load_words("nouns.txt")

    # Generate 4 random alphanumeric characters
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
    return f"{random.choice(_adjectives)}_{random.choice(_nouns)}_{suffix}"
