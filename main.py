import os
from app.celery.celery import celery_app


def main():
    from celery.bin import worker

    # 从环境变量读取配置
    loglevel = os.getenv("LOGLEVEL", "INFO")
    concurrency = int(os.getenv("CONCURRENCY", "4"))
    queue = os.getenv("TASK_QUEUE_NAME", "celery")

    # 创建并启动 worker
    worker = worker.worker(app=celery_app)
    worker.run(
        loglevel=loglevel,
        concurrency=concurrency,
        queues=[queue],
    )


if __name__ == "__main__":
    main()
