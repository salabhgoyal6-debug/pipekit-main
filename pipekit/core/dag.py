from collections import deque
from pipekit.core.state import TaskRun, TaskState
from pipekit.db.models import get_engine, init_db, TaskRunModel
from sqlalchemy.orm import Session
from pipekit.worker.worker import execute_task

class Dag:
    def __init__(self, name, db_url = "postgresql://aditya:1234@127.0.0.1:5432/pipekit" ):
        self.name = name 
        self.tasks = {}
        self.engine = get_engine(db_url)
        init_db(self.engine)
            
    def add_task(self, task):
        self.tasks[task.name] = task
        return self
    
    def _save_task_run(self, session, tr):
        # we are saving or updating taksrun to the database
        existing = session.query(TaskRunModel).filter_by(id=tr.id).first()
        if existing:
            existing.state = tr.state.value
            existing.started_at = tr.started_at
            existing.ended_at = tr.ended_at
            existing.result = str(tr.result) if tr.result else None
            existing.error = tr.error   
        else:
            session.add(TaskRunModel(
                id=tr.id,
                dag_name=self.name,
                task_name=tr.task_name,
                state=tr.state.value,
                started_at=tr.started_at,
                ended_at=tr.ended_at,
                result=str(tr.result) if tr.result else None,
                error=tr.error
            ))
        session.commit()
            
            
    def get_execution_order(self):
        degree = {task.name: 0 for task in self.tasks.values()}
        #it creates dictionary called degree where key = task.name and value = 0
        
        for task in self.tasks.values():
            for dep in task.dependencies:
                degree[task.name] += 1 
        # for every dependencies increase the count 
        
        # now we will find the task that are not dependent on any other task degree of that task = 0.
        
        queue = deque()
        
        for task_name, count in degree.items():
            if count == 0:
                queue.append(self.tasks[task_name])
        
        # now we will repeat take a task -> run it -> and then unlock others 
        
        stages = []
        
        while queue:
            stage = list(queue)
            stages.append(stage)
            
            next_queue = deque()
            
            for current in stage:
                for task in self.tasks.values():
                    if current in task.dependencies:
                        degree[task.name] -= 1
                        
                        if degree[task.name] == 0:
                            next_queue.append(task)
            
            queue = next_queue
        
        total_task = sum(len(stage) for stage in stages)
        if total_task != len(self.tasks):
            raise ValueError("cycle detected in DAG")

        return stages  
    
    def run(self):      
        # we will learn artifact store , what is artifact store,we cant just pass the python return vaue between the tasks. the data need to go somewhere both task can reach. that somewhere is called artifact store. it could be a database, a file, a redis
        
        stages = self.get_execution_order()
        artifact_store = {} # task name and  -> result 
        run_log = {} # task_name > TaskRun
        
        with Session(self.engine) as session:
            for i, stage in enumerate(stages):
                #we will send all tak in this stage to redis simulatneously
                futures = {}
                for task in stage:
                    tr = TaskRun(task_name= task.name)
                    run_log[task.name] = tr
                    tr.mark_running()
                    # save as pending 
                    self._save_task_run(session, tr)
                    
                    inputs = [artifact_store[dep.name] for dep in task.dependencies]
                    futures[task.name] = (execute_task.delay(task.name, inputs), tr)
                    
                for task_name, (futures, tr) in futures.items():
                        try:
                            result = futures.get(timeout=30)
                            artifact_store[task_name] = result
                            tr.mark_success(result)
                            self._save_task_run(session, tr)
                        except Exception as e:
                            tr.mark_failed(str(e))
                            self._save_task_run(session, tr)
                            print(f"{self.name} failed at task {task.name}")
                            return run_log # it will stop everything
        
        print(f"{self.name} completed")
        return run_log  