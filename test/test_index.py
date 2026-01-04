import pytest
from app.celery.tasks import index_document_task
from app.celery.celery import celery_app


@pytest.fixture(autouse=True)
def set_eager_mode(monkeypatch):
    """设置 Celery 为 eager 模式，同步执行任务用于测试"""
    monkeypatch.setenv("CELERY_TASK_ALWAYS_EAGER", "True")
    monkeypatch.setenv("CELERY_TASK_EAGER_PROPAGATES", "True")
    # 配置 eager 模式
    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
    )


def test_index_task_full_flow():
    """测试从接收到执行完成任务的全过程"""
    # 测试参数
    content_hash = "6aab2192674424aea61094d09693a6e41487030e04f1c97f8e1565c2e1106749"
    file_url = "https://my-qa-go-1313494932.cos.ap-shanghai.myqcloud.com/uploads-local/6aab2192674424aea61094d09693a6e41487030e04f1c97f8e1565c2e1106749.txt"
    file_type = "txt"

    # 执行任务（eager模式下同步执行）
    result = index_document_task(content_hash, file_url, file_type)

    # 验证任务成功完成
    assert result["status"] == "success"
    assert result["content_hash"] == content_hash
    assert result["message"] == "索引完成"
