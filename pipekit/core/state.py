from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
import uuid

class TaskState(Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    SUCCESS  = "success"
    FAILED   = "failed"
    RETRYING = "retrying"

#dataclass is pyhton decorator that automatically cretaes common methods.
@dataclass
class TaskRun:
    task_name: str
    state: TaskState = TaskState.PENDING
    started_at: datetime =None
    ended_at: datetime = None
    result: object = None
    error: str = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def mark_running(self):
        self.state = TaskState.RUNNING
        self.started_at = datetime.now()
        
    def mark_success(self, result):
        self.state = TaskState.SUCCESS
        self.ended_at = datetime.now()
        self.result = result
        
    
    def mark_failed(self, error):
        self.state = TaskState.FAILED
        self.finished_at = datetime.now()
        self.error = error
        
    def __repr__(self):
        return f"TaskRun(task={self.task_name}, state={self.state.value})"