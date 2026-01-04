from celery import Celery
import os

celery_app = Celery(
    os.getenv("CELERY_APP_NAME"),
    broker=os.getenv("BROKER_REDIS_URL"),
    backend=os.getenv("BACKEND_REDIS_URL"),
    include=["app.celery.tasks"],  # 确保导入任务模块
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
    task_acks_late=True,  # 任务执行完成后再确认，防丢失
    worker_prefetch_multiplier=1,  # 避免 Worker 一次抢太多耗时任务
)
