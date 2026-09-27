from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import requests

scheduler= BlockingScheduler()


BASE_URL = "http://127.0.0.1:8000/"

def trigger_pipeline(dag_name: str ):
    print(f"scheduler {datetime.now()} - triggering {dag_name}")
    response = requests.post(f"{BASE_URL}/pipeline/{dag_name}/run")
    data = response.json()
    print(f"scheduler {dag_name} finished : {data['status']}")
    
scheduler.add_job(
    trigger_pipeline,
    #interval 
    trigger="interval",
    seconds=10,
    args=["sexy_sexy_pipeline"]
)

#cron -> means everyday at 9am. 
scheduler.add_job(
    trigger_pipeline,
    trigger="cron",
    hour=9,
    minutes=0,
    args=["sexy_sexy_pipeline"]
)

if __name__ == "__main__":
    print("strating pipekit scheduler")
    scheduler.start()