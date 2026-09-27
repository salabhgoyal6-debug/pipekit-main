from pipekit.core.dag import Dag
from pipekit.tasks.etl_tasks import extract, transform, load

transform.depends_on(extract)
load.depends_on(extract)

dag = Dag("etl_pipeline")
dag.add_task(extract)
dag.add_task(transform)
dag.add_task(load)

if __name__ == "__main__":
    run_log = dag.run()
    print("\n--- Run Summary ---")
    for name, tr in run_log.items():
        print(tr)