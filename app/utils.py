import random
import string


def random_id(size: int) -> str:
    available_values = string.ascii_lowercase + string.digits
    return "".join(random.choice(available_values) for _ in range(size))


def to_pairs(values: list[str]) -> list[tuple[str, str]]:
    return list(zip(values[: len(values) // 2], values[len(values) // 2 :]))
