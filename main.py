from dotenv import load_dotenv
import os
from app.celery.celery import celery_app


def main():
    load_dotenv()
    print(f"broker url: {os.getenv('BROKER_REDIS_URL')}")
    print(f"backend url: {os.getenv('BACKEND_REDIS_URL')}")
    print(f"business url: {os.getenv('BUSINESS_REDIS_URL')}")
    # 从环境变量读取配置
    loglevel = os.getenv("LOGLEVEL", "INFO")
    concurrency = int(os.getenv("CONCURRENCY", "4"))
    queue = os.getenv("TASK_QUEUE_NAME", "celery")

    # 创建并启动 worker
    worker = celery_app.Worker(
        loglevel=loglevel,
        concurrency=concurrency,
        queues=[queue],
    )
    worker.start()


if __name__ == "__main__":
    main()
