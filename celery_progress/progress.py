
from .redis_utils import RedisTaskRepository, ChildTaskManager, IndividualTaskManager
import logging


CELERY_TASK_CONTEXT = {"celery_task": None}


class CeleryProgressContext:
    def __init__(self, celery_task, additional_data: dict=None):
        self.celery_task = celery_task
        
        logging.info(self.celery_task.request.headers)

    def __enter__(self):
        if not CELERY_TASK_CONTEXT["celery_task"]:
            CELERY_TASK_CONTEXT["celery_task"] = self.celery_task
            return
        raise RuntimeError("The context has already been initialized")

    def __exit__(self, *args, **kwargs):
        CELERY_TASK_CONTEXT["celery_task"] = None


def task_progress_update(current_amount: int, total_amount: int = None):

    logging.info("UPDATING TASK PROGRESS")
    if not CELERY_TASK_CONTEXT["celery_task"]:
        raise RuntimeError("CeleryProgresContext not initialized.")

    update_data = {"current_amount": current_amount}
    if total_amount:
        update_data["total_amount"] = total_amount
    
    celery_task = CELERY_TASK_CONTEXT["celery_task"] 

    task_id = celery_task.request.id
    parent_id = celery_task.request.parent_id

    if parent_id == task_id:
        IndividualTaskManager(task_id, RedisTaskRepository()).update_task(update_data)
        logging.info("INDIVIDUAL TASK ENCOUNTERED")
    else:
        ChildTaskManager(parent_id, task_id, RedisTaskRepository()).update_child(update_data)
        logging.info("INDIVIDUAL TASK ENCOUNTERED")