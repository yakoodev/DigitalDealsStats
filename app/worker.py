from redis import Redis
from rq import Queue, Worker

from app.core.config import get_settings


def run_worker() -> None:
    settings = get_settings()
    redis_conn = Redis.from_url(settings.redis_url)
    queue = Queue(settings.rq_queue_name, connection=redis_conn)
    worker = Worker([queue], connection=redis_conn)
    worker.work()


if __name__ == "__main__":
    run_worker()

