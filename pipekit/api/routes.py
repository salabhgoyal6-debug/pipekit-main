from fastapi import FastAPI
from pipekit.core.task import task
from pipekit.core.dag import Dag
from sqlalchemy.orm import Session
from pipekit.db.models import get_engine, init_db, TaskRunModel

app = FastAPI()

def build_etl_dag(dag_name):
    @task
    def extract():
        print("extracting raw data")
        return "raw_data"

    @task
    def transform(data):
        print(f"transforming: {data}")
        return data.upper()

    @task
    def load(data):
        print(f"loading: {data}")
        return f"saved({data})"

    transform.depends_on(extract)
    load.depends_on(transform)

    dag = Dag(dag_name)
    dag.add_task(extract)
    dag.add_task(transform)
    dag.add_task(load)
    return dag

@app.get("/")
def health_check():
    return {"status": "pipekit is running"}

@app.post("/pipeline/{dag_name}/run")
def run_pipeline(dag_name: str):
    dag = build_etl_dag(dag_name)
    run_log = dag.run()
    return {
        "dag": dag_name,
        "status": "completed",
        "tasks": {
            name: tr.state.value
            for name, tr in run_log.items()
        }
    }
    
@app.post("/pipeline/{dag_name}/status")
def status_pipeline(dag_name: str):
    engine = get_engine()
    with Session(engine) as session:
        runs = (session.query(TaskRunModel).filter_by(dag_name=dag_name).order_by(TaskRunModel.started_at.desc()).limit(10).all()
                )
        
        return {
            "dag": dag_name,
            "runs": [
                {
                    "task_name": r.task_name,
                    "state": r.state,
                    "started_at": str(r.started_at),
                    "result": r.result
                }
                for r in runs
            ]
        }