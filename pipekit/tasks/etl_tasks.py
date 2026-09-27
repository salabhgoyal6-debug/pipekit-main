from pipekit.core.task import task
import time

@task
def extract():
    print("extracting raw data")
    time.sleep(2)
    return "raw_data"

@task
def transform(data):
    print("START transform")
    time.sleep(5)
    print("END transform")
    return data.upper()

@task
def load(data):
    print("START load")
    time.sleep(5)
    print("END load")
    return f"saved({data})"