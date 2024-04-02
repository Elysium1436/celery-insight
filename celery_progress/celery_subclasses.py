
from datetime import datetime
import logging
from celery import Task, current_task, uuid
from .redis_utils import ChildTaskManager, RedisTaskRepository, ParentTaskManager, IndividualTaskManager
import uuid


class ChildTask(Task):
    """Task that stores it's id on the 'group' meta field"""

    def apply_async(self, total_amount, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, shadow=None, additional_metadata = None, **options):

        parent_id = current_task.request.id

        if parent_id is None:
            raise RuntimeError("Need to be run inside a task context. Perhaps you're not running from the ParentTask class?")
        
        metadata = additional_metadata or {}
        task_id = task_id or str(uuid.uuid4())
        metadata["total_amount"] = total_amount
        metadata["current_amount"] = 0
        metadata["time_deployed"] = datetime.now().isoformat()
        metadata["state"] = "PROGRESS"
        ChildTaskManager(parent_id, task_id).update_child(metadata)

        task_result = super().apply_async(args, kwargs, task_id, producer, link, link_error, shadow)

        return task_result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        update_metadata = {"state": status}
        parent_id = self.request.parent_id
        if einfo:
            update_metadata["einfo"] = einfo
        ChildTaskManager(parent_id, task_id).update_child(update_metadata)
        
        return super().after_return(status, retval, task_id, args, kwargs, einfo)

class ParentTask(Task):
    """Task that stores it's id on the 'group' meta field"""

    def apply_async(self,  args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, shadow=None, additional_metadata=None **options):

        task_id = task_id or str(uuid.uuid4())
        additional_metadata = additional_metadata or {}
        additional_metadata.update({"time_deployed": datetime.now().isoformat(), "state": "PROGRESS"})
        ParentTaskManager(task_id).set_parent(additional_metadata)

        task_result = super().apply_async(args, kwargs, task_id, producer, link, link_error, shadow)

        return task_result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        update_metadata = {"state": status, "time_finished": datetime.now().isoformat()}
        if einfo:
            update_metadata["einfo"] = einfo
        ParentTaskManager(task_id).update_parent(update_metadata)
        
        return super().after_return(status, retval, task_id, args, kwargs, einfo)


class IndividualTask(Task):

    def apply_async(self, total_amount,  args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, shadow=None, additional_metadata=None, **options):

        additional_metadata = additional_metadata or {}
        additional_metadata["state"] = "PROGRESS"
        additional_metadata["time_deployed"] = datetime.now().isoformat()
        additional_metadata["current_amount"] = 0
        additional_metadata["total_amount"] = total_amount
        task_id = task_id or str(uuid.uuid4())
        IndividualTaskManager(task_id).set_task(additional_metadata)
        task_result = super().apply_async(args, kwargs, task_id, producer, link, link_error, shadow, **options)
        return task_result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        update_metadata = {"state": status, "time_finished": datetime.now().isoformat()}
        if einfo:
            update_metadata["einfo"] = einfo
        IndividualTaskManager(task_id).update_task(update_metadata)
        
        return super().after_return(status, retval, task_id, args, kwargs, einfo)