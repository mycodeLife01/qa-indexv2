import logging
import redis
from celery import Task
from app.celery.celery import celery_app
from app.index import process
import os

# 初始化一个独立的 Redis 客户端用于操作锁
# 注意：不要直接用 celery.backend.client，因为它可能被配置为其他 Backend
redis_client = redis.from_url(os.getenv("BUSINESS_REDIS_URL"), decode_responses=True)
logger = logging.getLogger(__name__)


class IndexTask(Task):
    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        if args and len(args) > 0:
            content_hash = args[0]
            lock_key = f"index_lock:{content_hash}"
            redis_client.delete(lock_key)
            logger.info(f"释放去重锁: {lock_key}")
        super().after_return(status, retval, task_id, args, kwargs, einfo)


@celery_app.task(bind=True, base=IndexTask, name="index_document")
def index_document_task(self, content_hash, file_url, file_type):

    logger.info(f"开始处理任务: {self.request.id}, Hash: {content_hash}")

    self.update_state(state="PROCESSING", meta={"progress": 0})

    try:
        process(file_url, content_hash, file_type)

        return {
            "status": "success",
            "content_hash": content_hash,
            "message": "索引完成",
        }

    except Exception as e:
        logger.error(f"任务失败: {e}")
        raise self.retry(exc=e, countdown=5 * (2**self.request.retries), max_retries=3)
