class RedisError(Exception):
    message: str


class StreamIdOrderError(RedisError):
    message = "The ID specified in XADD is equal or smaller than the target stream top item"


class StreamIDTooLowError(RedisError):
    message = "The ID specified in XADD must be greater than 0-0"
