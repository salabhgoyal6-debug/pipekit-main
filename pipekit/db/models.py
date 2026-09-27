from sqlalchemy import create_engine, Column, String,DateTime, Text
# create engine , that will help to communicate between database and our application.
from sqlalchemy.orm import declarative_base, Session
from datetime import datetime

Base = declarative_base()

class TaskRunModel(Base):
    __tablename__ = "task_runs"
    
    id = Column(String, primary_key=True)
    dag_name = Column(String, nullable=False)
    task_name = Column(String, nullable=False)
    state = Column(String, nullable=False, default="pending")
    started_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    

def get_engine(url="postgresql://aditya:1234@127.0.0.1:5432/pipekit"):
    return create_engine(url)

def init_db(engine):
    Base.metadata.create_all(engine)