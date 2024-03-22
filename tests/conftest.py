from celery_progress.redis_utils import RedisTaskRepository, ParentTaskManager, ChildTaskManager, IndividualTaskManager
from celery_progress.celery_subclasses import IndividualTask, ChildTask
from celery_progress.progress import CeleryProgressContext, task_progress_update, task_progress_increment
from celery import Celery
import pytest
import json

class MockRedisClient:
    def __init__(self):
        self.storage = {}
    
    def hset(self, store_key: str, key: str, value: str):
        self.storage[store_key] = {key.encode(): value.encode()}

    def hget(self, store_key: str, key: str ):
        return self.storage[store_key][key.encode()]

    def hgetall(self, store_key: str):
        return self.storage[store_key]

@pytest.fixture
def celery_app_task():
    app = Celery('test')
    app.conf.task_always_eager = True


    @app.task(base=IndividualTask, bind=True)
    def add(self, x, y):
        with CeleryProgressContext(self):
            task_progress_update(3, 5)
        return x+y

    return app, add


@pytest.fixture
def increment_task_individual(celery_app_task):
    app, _ = celery_app_task

    @app.task(base=IndividualTask, bind=True)
    def task_increment(self):
        with CeleryProgressContext(self):
            task_progress_increment()
    
    return task_increment

@pytest.fixture
def increment_task_child(celery_app_task):
    app, _ = celery_app_task

    @app.task(base=ChildTask, bind=True)
    def task_increment(self):
        with CeleryProgressContext(self):
            task_progress_increment()
    
    return task_increment



@pytest.fixture
def redis_client():
    return MockRedisClient()

@pytest.fixture
def redis_repo(redis_client):
    print(type(redis_client))
    return RedisTaskRepository(redis_client=redis_client)
