
from .redis_utils import RedisTaskRepository, ChildTaskManager, IndividualTaskManager
from .celery_subclasses import IndividualTask, ChildTask, ParentTask
import logging


CELERY_TASK_CONTEXT = {"celery_task": None}


class CeleryProgressContext:
    def __init__(self, celery_task):
        self.celery_task = celery_task
        logging.info(type(celery_task))
        
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

    celery_task = CELERY_TASK_CONTEXT["celery_task"] 

    update_data = {"current_amount": current_amount}
    if total_amount:
        update_data["total_amount"] = total_amount
    

    if isinstance(celery_task, IndividualTask):
        IndividualTaskManager(celery_task.request.id).update_task(update_data)
    elif isinstance(celery_task, ChildTask):
        task_id = celery_task.request.id
        parent_id = celery_task.request.parent_id
        ChildTaskManager(parent_id, task_id).update_child(update_data)


def task_progress_increment():
    if not CELERY_TASK_CONTEXT["celery_task"]:
        raise RuntimeError("CeleryProgresContext not initialized.")

    celery_task = CELERY_TASK_CONTEXT["celery_task"] 

    # get the data
    if isinstance(celery_task, IndividualTask):
        metadata = IndividualTaskManager(celery_task.request.id).get_task_metdata()
        metadata["current_amount"] = metadata.get("current_amount", 0) + 1
        IndividualTaskManager(celery_task.request.id).update_task(metadata)
    if isinstance(celery_task, ChildTask):
        metadata = ChildTaskManager(celery_task.request.parent_id, celery_task.request.id).get_task_metadata()
        metadata["current_amount"] = metadata.get("current_amount", 0) + 1
        ChildTaskManager(celery_task.request.parent_id, celery_task.request.id).update_child(metadata)

