from pipekit.core.task import task
import time

@task
def fetch_users():
    print("fetching users...")
    time.sleep(2)  # simulate slow API call
    return "users_data"

@task
def fetch_orders():
    print("fetching orders...")
    time.sleep(2)  # simulate slow API call
    return "orders_data"

@task
def fetch_products():
    print("fetching products...")
    time.sleep(2)  # simulate slow API call
    return "products_data"

@task
def merge(users, orders, products):
    print(f"merging: {users}, {orders}, {products}")
    return f"merged({users}, {orders}, {products})"

@task
def save(data):
    print(f"saving: {data}")
    return "done"