import click
import requests

BASE_URL = "http://127.0.0.1:8000/"

@click.group()
def cli():
    "pipekit"
    pass
    # thsi creates a command group, like pipekit [command]
    
#run command 
@cli.command()
@click.argument("dag_name")
def run(dag_name):
    #this will trigger the pipeline. 
    click.echo(f"[pipekit] Triggering pipeline: {dag_name}")
    response = requests.post(f"{BASE_URL}/pipeline/{dag_name}/run")
    data = response.json()
    click.echo(f"[pipekit] Status: {data['status']}")
    for task_name, state in data["tasks"].items():
        click.echo(f"  {task_name}: {state}")


#status command 
@cli.command()
@click.argument("dag_name")
def status(dag_name):
    # get status
    response = requests.post(f"{BASE_URL}/pipeline/{dag_name}/status")
    data = response.json()
    for run in data["runs"]:
        click.echo(f"  {run['task_name']}: {run['state']}")


if __name__ == "__main__":
    cli()