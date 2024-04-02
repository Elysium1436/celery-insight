import pytest
from celery_progress.celery_subclasses import ChildTask, ParentTask, IndividualTask
from celery_progress.redis_utils import ParentTaskManager, RedisTaskRepository, ChildTaskManager, IndividualTaskManager
from celery import Celery, Task, current_task
from unittest.mock import patch, MagicMock

    

@patch.object(Task, 'apply_async', return_value="None")
def test_apply_parent_task(mocked_patch_object, celery_app_task, redis_repo):
    app, add = celery_app_task
    add.apply_async(args=[1,1], task_id="1234")
    assert ParentTaskManager("1234").get_parent()
    assert "1234" in RedisTaskRepository().retrieve_all_tasks()


@patch.object(Task, "after_return", return_value=None)
@patch.object(ParentTaskManager, "update_parent")
def test_after_return_task(mocked_update_parent, mocked_after_return, celery_app_task, redis_repo):
    app, add = celery_app_task
    add.after_return("FINISHED", "123", "uuid", [], {}, None)
    args = mocked_update_parent.call_args

    assert args[0][0]["state"]=="FINISHED"
    assert "time_finished" in args[0][0]


@patch("celery_progress.celery_subclasses.current_task", create=True)
@patch.object(Task, 'apply_async', return_value="None")
@patch.object(ChildTaskManager, "update_child")
def test_apply_child_task(mocked_update_child, mocked_apply_async, mocked_current_task,  redis_repo):
    # Mocking the reference to the parent task, since there are no parent task
    mocked_request = MagicMock()
    mocked_request.id = "some_id"
    mocked_current_task.return_value.request = mocked_request
    mocked_current_task.request = mocked_request
    ChildTask().apply_async(200)
    metadata = mocked_update_child.call_args[0][0]
    assert metadata["total_amount"] == 200
    assert metadata["current_amount"] == 0
    assert "time_deployed" in metadata
    assert metadata["state"] == "PROGRESS"



@patch.object(Task, 'apply_async', return_value="None")
@patch.object(IndividualTaskManager, "set_task")
def test_apply_individual_task(update_task_mock, apply_async_mock, redis_repo):
    IndividualTask().apply_async(total_amount=200, task_id="1234")
    print(update_task_mock)
    metadata = update_task_mock.call_args[0][0]
    assert metadata["total_amount"] == 200
    assert metadata["current_amount"] == 0
    assert "time_deployed" in metadata
    assert metadata["state"] == "PROGRESS"

    apply_async_args = apply_async_mock.call_args[0]
    assert "1234" == apply_async_args[2]


