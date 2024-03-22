from celery_progress.celery_subclasses import ParentTask, ChildTask
from celery_progress.progress import CeleryProgressContext, task
from celery import shared_task





@shared_task(base=ParentTask)
def parent_task(self, *args, **kwargs):

    batch_ids = get_batch_ids(...)

    for batch in batch_ids:
        child_task.apply_async(...)



@shared_task(base=ChildTask, bind=True)
def child_task(self):
    
    with CeleryProgressContext(self):
        your_function()


