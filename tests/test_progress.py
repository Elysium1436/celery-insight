import pytest
from celery_progress.redis_utils import RedisTaskRepository
from celery_progress.redis_utils import IndividualTaskManager, ChildTaskManager
from unittest.mock import patch, MagicMock





def test_progress_update(celery_app_task, redis_repo):
    app, add = celery_app_task
    add.apply_async(total_amount=300, task_id="1234", args=[1,1]).get()

    metadata = redis_repo.retrieve_specific_task_meta("1234")

    assert metadata["current_amount"] == 3
    assert metadata["total_amount"] == 5


def test_task_individual_increment(increment_task_individual, redis_repo):
    result = increment_task_individual.apply_async(total_amount=100)
    metadata = IndividualTaskManager(result.id).get_task_metdata()
    assert metadata["current_amount"] == 1
    assert metadata["total_amount"] == 100
    
@patch("celery_progress.celery_subclasses.current_task", create=True)
def test_task_child_increment(mocked_current_task, increment_task_child, redis_repo):
    mocked_request = MagicMock()
    mocked_request.id = "1234"
    mocked_current_task.return_value.request = mocked_request
    mocked_current_task.request = mocked_request
    result = increment_task_child.apply_async(total_amount=100)
    metadata = ChildTaskManager("1234", result.id).get_task_metadata()
    assert metadata["current_amount"] == 1
    assert metadata["total_amount"] == 100