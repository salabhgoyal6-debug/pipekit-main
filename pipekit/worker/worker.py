from celery import Celery
from pipekit.core.task import TASK_REGISTRY

app = Celery(
    "pipekit",
    broker="redis://localhost:6379/0", # where to add task
    backend="redis://localhost:6379/0" # where results of tasks are saved 
)

@app.task # it converts this function into a remote executable jobs
def execute_task(task_name: str, inputs: list):
    
    #imported inside function to avoid circular imports.
    import pipekit.tasks.etl_tasks
    import pipekit.tasks.parallel_tasks
    
    task = TASK_REGISTRY[task_name]
    return task(*inputs)
