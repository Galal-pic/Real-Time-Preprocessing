from redis import Redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379


def get_redis():
    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)
    return redis_client
