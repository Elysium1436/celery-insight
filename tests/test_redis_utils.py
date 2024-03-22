import pytest
from celery_progress.redis_utils import RedisTaskRepository, ParentTaskManager, ChildTaskManager, IndividualTaskManager
import json
# Redis Repo

def test_set_task(redis_repo, redis_client):
    redis_repo.set_task("123", {"456": "789"})
    print(redis_client.hget("CELERY_TASKS","123").decode("utf-8"))
    assert json.loads(redis_client.hget("CELERY_TASKS","123").decode("utf-8")) == {"456": "789"}

def test_get_task(redis_repo):
    redis_repo.set_task("123", {"456": "789"})
    metadata = redis_repo.retrieve_specific_task_meta("123")
    assert metadata["456"] == "789"

# Parent task manager

@pytest.fixture
def parent_task_manager(redis_repo):
    return ParentTaskManager("parent_id")

def test_if_parent_set(redis_repo, parent_task_manager):
    parent_metadata = {"key": "value"}
    parent_task_manager.set_parent(parent_metadata)
    assert redis_repo.retrieve_specific_task_meta("parent_id")["key"] == "value"
    assert redis_repo.retrieve_specific_task_meta("parent_id")["children_tasks"]  == {}


def test_update_parent_task(redis_repo, parent_task_manager):
    parent_metadata = {"key": "value"}
    parent_task_manager.set_parent(parent_metadata)
    update_metadata = {"update_key": "value"}
    parent_task_manager.update_parent(update_metadata)
    assert redis_repo.retrieve_specific_task_meta("parent_id")["update_key"] == "value"


def test_set_parent_child(parent_task_manager):
    parent_task_manager.set_parent({"key": "value"})
    parent_task_manager.set_child("child_id", {"key":"child_value"})
    assert parent_task_manager.get_parent()["children_tasks"]["child_id"]["key"] == "child_value"

def test_update_parent_child(parent_task_manager):
    parent_task_manager.set_parent({"key": "value"})
    parent_task_manager.set_child("child_id", {"key":"child_value"})
    parent_task_manager.update_child("child_id", {"update_key": "update_child_value"})
    assert parent_task_manager.get_parent()["children_tasks"]["child_id"]["update_key"] == "update_child_value"


# Child task manager
    
@pytest.fixture
def child_task_manager(parent_task_manager):
    parent_task_manager.set_parent({"key": "value"})
    parent_task_manager.set_child("child_id", {"key":"child_value"})
    return ChildTaskManager(parent_task_manager.parent_id, "child_id")


def test_set_child(parent_task_manager, child_task_manager):
    child_task_manager.set_child({"key": "child_value"})
    assert parent_task_manager.get_parent()["children_tasks"]["child_id"]["key"] == "child_value"

def test_update_child(parent_task_manager, child_task_manager):
    child_task_manager.set_child({"key": "value"})
    child_task_manager.update_child({"update_key":"update_value"})
    assert parent_task_manager.get_parent()["children_tasks"]["child_id"]["update_key"] == "update_value"

# Individual task manager

@pytest.fixture
def individual_task_manager(redis_repo):
    return IndividualTaskManager("individual_id")

def test_set_task(redis_repo, individual_task_manager):
    individual_task_manager.set_task({"key":"value"})
    assert redis_repo.retrieve_specific_task_meta("individual_id")["key"] == "value"

def test_update_task(redis_repo, individual_task_manager):
    individual_task_manager.set_task({"key":"value"})
    individual_task_manager.update_task({"update_key":"value"})
    assert redis_repo.retrieve_specific_task_meta("individual_id")["update_key"] == "value"