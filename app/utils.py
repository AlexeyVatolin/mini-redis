import random
import string


def random_id(size: int) -> str:
    available_values = string.ascii_lowercase + string.digits
    return "".join(random.choice(available_values) for _ in range(size))
