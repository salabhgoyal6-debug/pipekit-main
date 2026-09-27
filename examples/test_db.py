from pipekit.db.models import get_engine, init_db, TaskRunModel
from sqlalchemy.orm import Session
import uuid
from datetime import datetime

# connect database
engine = get_engine()
init_db(engine)
print("tables created")


# insert some data or rows, 
with Session(engine) as session:
    run = TaskRunModel(
        id = str(uuid.uuid4()),
        dag_name = "etl_pipeline",
        task_name = "extract",
        state = "success",
        started_at = datetime.now(), 
        ended_at = datetime.now(),
        result = "raw_data"
        )
    session.add(run)
    session.commit()
    print("row inserted")


# we will read
with Session(engine) as session:
    runs = session.query(TaskRunModel).all()
    for r in runs:
        print(f"{r.dag_name, r.task_name, r.state, r.result}")