from pipekit.core.dag import Dag
from pipekit.tasks.parallel_tasks import fetch_users, fetch_orders, fetch_products, merge, save

merge.depends_on(fetch_users, fetch_orders, fetch_products)
save.depends_on(merge)

dag = Dag("parallel_pipeline")
dag.add_task(fetch_users)
dag.add_task(fetch_orders)
dag.add_task(fetch_products)
dag.add_task(merge)
dag.add_task(save)

if __name__ == "__main__":
    import time
    start = time.time()
    dag.run()
    print(f"\nTotal time: {time.time() - start:.2f}s")