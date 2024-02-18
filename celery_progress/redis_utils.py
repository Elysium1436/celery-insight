import redis
import json
from copy import deepcopy
from celery.signals import worker_ready, task_success
import logging

def deep_update(original, updated):
    for key, value in updated.items():
        if isinstance(value, dict) and key in original:
            deep_update(original[key], updated[key])
        else:
            original[key] = value


class RedisInitializer:
    @staticmethod
    def initialize_redis_client(redis_url: str):
        RedisTaskRepository(redis_url)


class RedisTaskRepository:
    task_key = "CELERY_TASKS"
    _redis_client = None  # Class-level attribute to cache the Redis client

    def __init__(self, redis_url: str = None):
        if RedisTaskRepository._redis_client is None:
            if redis_url is None:
                raise RuntimeError("Redis client is not initialized and yet no url was given")
            RedisTaskRepository._initialize_redis_client(redis_url)
        self.redis_client = RedisTaskRepository._redis_client

    @classmethod
    def _get_redis_client(cls):
        return cls._redis_client


    @classmethod
    def _initialize_redis_client(cls, redis_url: str):
        """Initialize the Redis client if it hasn't been initialized yet."""
        if cls._redis_client is None:
            cls._redis_client = redis.StrictRedis.from_url(redis_url)

    def set_task(self, task_id: str, metadata: dict):
        serialized_data = json.dumps(metadata)
        self.redis_client.hset(self.task_key, task_id, serialized_data)

    def update_task(self, task_id: str, metadata: dict):
        current_metadata = self.retrieve_specific_task_meta(task_id)
        deep_update(current_metadata, metadata)
        self.set_task(task_id, current_metadata)

    def retrieve_specific_task_meta(self, task_id):
        serialized_data = self.redis_client.hget(self.task_key, task_id).decode("utf-8")
        return json.loads(serialized_data)

    def retrieve_all_tasks(self):
        all_tasks = self.redis_client.hgetall(self.task_key)

        tasks = {k.decode('utf-8'):json.loads(v.decode('utf-8')) for k, v in all_tasks.items()}
        return tasks


class ParentTaskManager:
    def __init__(self, parent_id, redis_repo: RedisTaskRepository):
        self.parent_id = parent_id
        self.redis_repo = redis_repo

    def set_parent(self, parent_metadata: dict = {}):
        metadata = deepcopy(parent_metadata)
        metadata["children_tasks"] = {}
        self.redis_repo.set_task(self.parent_id, metadata)
    
    def update_parent(self, parent_metadata: dict):
        self.redis_repo.update_task(self.parent_id, parent_metadata)

    def set_child(self, child_id, child_metadata: dict):
        parent_info = self.redis_repo.retrieve_specific_task_meta(self.parent_id)
        parent_info["children_tasks"][child_id] = child_metadata
        self.redis_repo.set_task(self.parent_id, parent_info)

    def update_child(self, child_id, new_child_metadata:dict):
        new_metadata = {child_id: new_child_metadata}
        formatted_metadata = {"children_tasks": new_metadata}
        self.redis_repo.update_task(self.parent_id, formatted_metadata)


class ChildTaskManager:
    def __init__(self, parent_id, child_id, redis_repo: RedisTaskRepository):
        self.child_id = child_id
        self.parent_task_manager = ParentTaskManager(parent_id, redis_repo)

    def set_child(self, child_metadata):
        self.parent_task_manager.set_child(self.child_id, child_metadata)
    
    def update_child(self, new_child_metadata):
        self.parent_task_manager.update_child(self.child_id, new_child_metadata)


class IndividualTaskManager:
    def __init__(self, task_id, redis_repo: RedisTaskRepository):
        self.task_id = task_id
        self.redis_repo = redis_repo

    def set_task(self, task_metadata):
        self.redis_repo.set_task(self.task_id, task_metadata)
    
    def update_task(self, task_metadata):
        self.redis_repo.update_task(self.task_id, task_metadata)

"""
@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logging.info("Worker is ready. Checking if it's the first worker...")

    # Create an inspector instance to query active workers
    try:
        inspector = Inspect(app=celery_app)

        # Get the list of active workers
        active_workers = inspector.active()
        logging.info(f"{active_workers}")
    except Exception as e:
        logging.exception("yeet")
        logging.info(e)
    

    # Check if the current worker is the only one
    if active_workers is None:
        logging.info("This is the first worker. Clearing Redis database...")
        # Connect to Redis and clear the database
        redis_client.flushdb()  # Warning: This clears the entire database!



@task_success.connect
def store_task_info(sender, result, **kwargs):
    task_id = sender.request.id
    result = AsyncResult(task_id)
"""