from pipekit.worker.worker import execute_task

# .delay() sends the task to Redis instead of running it directly
result = execute_task.delay("extract", [])

print(f"Task ID: {result.id}")
print(f"Waiting for result...")
output = result.get(timeout=10)  # wait up to 10s for worker to finish
print(f"Result: {output}")