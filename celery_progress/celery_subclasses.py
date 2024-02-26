
from datetime import datetime
import logging
from celery import Task, current_task, uuid
from .redis_utils import ChildTaskManager, RedisTaskRepository, ParentTaskManager, IndividualTaskManager


class ChildTask(Task):
    """Task that stores it's id on the 'group' meta field"""

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, shadow=None, additional_metadata = None, **options):

        additional_metadata = {"time_deployed": datetime.now().isoformat(), "time_finished": None}
        parent_id = current_task.request.id

        if parent_id is None:
            raise RuntimeError("Need to be run inside a task context. Perhaps you're not running from the ParentTask class?")
        
        metadata = additional_metadata or {}
        metadata["total_amount"] = len(args[0])
        metadata["current_amount"] = 0
        metadata["time_deployed"] = datetime.now().isoformat()

        task_result = super().apply_async(args, kwargs, task_id, producer, link, link_error, shadow)
        metadata["state"] = task_result.state

        ChildTaskManager(parent_id, task_result.id, RedisTaskRepository()).update_child(metadata)

        return task_result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        update_metadata = {"state": status}
        parent_id = self.request.parent_id
        if einfo:
            update_metadata["einfo"] = einfo
        ChildTaskManager(parent_id, task_id, RedisTaskRepository()).update_child(update_metadata)
        
        return super().after_return(status, retval, task_id, args, kwargs, einfo)

class ParentTask(Task):
    """Task that stores it's id on the 'group' meta field"""

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, shadow=None, amount_name: str= "Iterações", **options):

        additional_metadata = {"time_deployed": datetime.now().isoformat(), "time_finished": None}
        logging.info(additional_metadata)

        task_result = super().apply_async(args, kwargs, task_id, producer, link, link_error, shadow)
        
        additional_metadata["state"] = task_result.state
        ParentTaskManager(task_result.id, RedisTaskRepository()).set_parent(additional_metadata)

        return task_result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        update_metadata = {"state": status, "time_finished": datetime.now().isoformat()}
        if einfo:
            update_metadata["einfo"] = einfo
        ParentTaskManager(task_id, RedisTaskRepository()).update_parent(update_metadata)
        
        return super().after_return(status, retval, task_id, args, kwargs, einfo)


class IndividualTask(Task):

    def apply_async(self, metadata: dict = {}, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, shadow=None, **options):


        additional_metadata = {"time_deployed": datetime.now().isoformat(), "time_finished": None}

        metadata = metadata.copy()

        task_result = super().apply_async(args, kwargs, task_id, producer, link, link_error, shadow)
        
        metadata["state"] = task_result.state
        IndividualTaskManager(task_result.id, RedisTaskRepository()).set_task(metadata)
        return task_result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        update_metadata = {"state": status, "time_finished": datetime.now().isoformat()}
        if einfo:
            update_metadata["einfo"] = einfo
        IndividualTaskManager(task_id, RedisTaskRepository()).update_task(update_metadata)
        
        return super().after_return(status, retval, task_id, args, kwargs, einfo)