import random
import string


def random_id(size: int) -> str:
    available_values = list(string.ascii_lowercase) + list(map(str, range(0, 10)))
    return "".join(random.choice(available_values) for _ in range(size))
