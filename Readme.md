# Pipekit 🚀

A distributed data pipeline orchestrator built from scratch in Python — similar to Apache Airflow but simplified to deeply understand the internals.

> Built to learn: DAG execution, state machines, distributed workers, retry logic, and task scheduling — from first principles.

---

## Demo

```bash
# trigger a pipeline
$ pipekit run etl_pipeline

[pipekit] Starting DAG: etl_pipeline
[pipekit] Stage 1: ['extract']
[pipekit] Starting task: extract
extracting raw data
[pipekit] Finished task: extract in 0.00s
[pipekit] Stage 2: ['transform']
[pipekit] Starting task: transform
transforming data: raw_data
[pipekit] Finished task: transform in 0.00s
[pipekit] Stage 3: ['load']
[pipekit] Starting task: load
loading data: RAW_DATA
[pipekit] Finished task: load in 0.00s
etl_pipeline completed

# check status
$ pipekit status etl_pipeline
  extract: success
  transform: success
  load: success
```

---

## Architecture

```
CLI / REST API / Scheduler
         │
         ▼
    DAG Engine
  (Kahn's algorithm → wave-based execution order)
         │
         ▼
  Task Queue (Redis + Celery)
  (dispatch entire wave simultaneously)
         │
    ┌────┴────┐
    ▼         ▼
 Worker 1   Worker 2  ...  Worker N
    │         │
    └────┬────┘
         ▼
  State Store (PostgreSQL)
  (every transition persisted)
```

### How it works

1. You define tasks with `@task` and express dependencies with `.depends_on()`
2. The DAG engine runs **Kahn's algorithm** to produce a wave-based execution order
3. Tasks in the same wave are dispatched to Redis **simultaneously**
4. Celery workers pick them up and execute in **parallel across processes**
5. Every state transition (`pending → running → success/failed`) is written to **PostgreSQL**
6. Task outputs flow to downstream tasks via an **artifact store**

---

## Features

- **`@task` decorator** — wrap any Python function into a trackable pipeline task
- **DAG system** — express dependencies, auto-resolve execution order
- **Wave-based parallelism** — tasks with no inter-dependencies run simultaneously
- **Distributed workers** — Celery + Redis for true multi-process execution
- **State machine** — `pending → running → success / failed / retrying`
- **Exponential backoff retry** — `2¹s, 2²s, 2³s...` between attempts
- **Artifact passing** — task outputs automatically become downstream inputs
- **Persistent state** — full audit trail in PostgreSQL
- **REST API** — trigger and monitor pipelines over HTTP (FastAPI)
- **CLI tool** — `pipekit run`, `pipekit status`
- **Cron scheduler** — run pipelines automatically on a schedule
- **Cycle detection** — raises an error immediately if your DAG has circular dependencies

---

## Proof of Parallelism

Three tasks each sleeping 2 seconds:

| Execution | Time |
|-----------|------|
| Sequential (naive) | 6.2s |
| Parallel (pipekit) | **2.04s** |

Three separate Celery workers — `ForkPoolWorker-7`, `ForkPoolWorker-8`, `ForkPoolWorker-1` — picked up all three tasks at the same timestamp and finished simultaneously.

---

## Project Structure

```
pipekit/
├── pipekit/
│   ├── core/
│   │   ├── task.py        # @task decorator + Task class + TASK_REGISTRY
│   │   ├── dag.py         # DAG class + Kahn's algorithm + run()
│   │   └── state.py       # TaskRun state machine
│   ├── scheduler/
│   │   └── scheduler.py   # APScheduler — cron + interval triggers
│   ├── worker/
│   │   └── worker.py      # Celery worker + execute_task
│   ├── api/
│   │   └── routes.py      # FastAPI routes
│   ├── db/
│   │   └── models.py      # SQLAlchemy models (TaskRunModel)
│   ├── cli/
│   │   └── cli.py         # Click CLI (run, status)
│   └── tasks/
│       ├── etl_tasks.py        # example ETL pipeline tasks
│       └── parallel_tasks.py   # example parallel pipeline tasks
├── examples/
│   ├── hello_pipeline.py
│   └── parallel_pipeline.py
├── tests/
├── pyproject.toml
└── README.md
```

---

## Tech Stack

| Technology | Role |
|------------|------|
| Python | Core language |
| FastAPI | REST API |
| PostgreSQL | State storage (persistent, ACID) |
| Redis | Task broker (message queue) |
| Celery | Distributed worker system |
| SQLAlchemy | ORM for database models |
| APScheduler | Cron + interval scheduling |
| Click | CLI framework |

---

## Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL running locally
- Redis running locally

### Installation

```bash
# clone the repo
git clone https://github.com/yourusername/pipekit.git
cd pipekit

# create virtual environment
python -m venv venv
source venv/bin/activate

# install in development mode
pip install -e .
```

### Setup Database

```bash
psql -U postgres -c "CREATE DATABASE pipekit;"
```

### Run Everything

You need 3 terminals:

**Terminal 1 — API server:**
```bash
uvicorn pipekit.api.routes:app --reload
```

**Terminal 2 — Celery worker:**
```bash
celery -A pipekit.worker.worker worker --loglevel=info
```

**Terminal 3 — trigger a pipeline:**
```bash
pipekit run etl_pipeline
```

---

## Usage

### Define a pipeline

```python
from pipekit.core.task import task
from pipekit.core.dag import Dag

@task
def extract():
    return "raw_data"

@task(retries=3)
def transform(data):
    return data.upper()

@task
def load(data):
    return f"saved({data})"

# express dependencies
transform.depends_on(extract)
load.depends_on(transform)

# build and run
dag = Dag("my_pipeline")
dag.add_task(extract)
dag.add_task(transform)
dag.add_task(load)
dag.run()
```

### Parallel pipeline

```python
@task
def fetch_users(): return "users"

@task
def fetch_orders(): return "orders"

@task
def merge(users, orders):
    return f"merged({users},{orders})"

# fetch_users and fetch_orders run in parallel
merge.depends_on(fetch_users, fetch_orders)
```

### Trigger via API

```bash
# run a pipeline
curl -X POST http://localhost:8000/pipelines/my_pipeline/run

# check status
curl http://localhost:8000/pipelines/my_pipeline/status

# interactive docs
open http://localhost:8000/docs
```

### Schedule automatically

```python
from pipekit.scheduler.scheduler import scheduler

# every 10 seconds
scheduler.add_job(trigger_pipeline, trigger="interval", seconds=10, args=["my_pipeline"])

# every day at 9am
scheduler.add_job(trigger_pipeline, trigger="cron", hour=9, minute=0, args=["my_pipeline"])
```

---

## Key Concepts Learned

**DAG Execution** — A task is "ready" only when ALL its dependencies have finished. Kahn's algorithm finds this automatically by tracking how many unfinished dependencies each task has.

**State Machines** — Reliability in distributed systems comes from state. Without persistent state, a crash means you lose everything. With it, you can observe, recover, and retry.

**Exponential Backoff** — If a task fails due to an overloaded service, retrying immediately makes things worse. Waiting `2ⁿ` seconds gives the service time to recover.

**Idempotency** — Each run creates new UUID-based records. Reruns don't overwrite history. Task functions themselves should be written idempotently (upserts over inserts).

**Why PostgreSQL over files** — Concurrency (multiple workers write safely), queryability (complex queries in one line), ACID guarantees (crash-safe writes).

**Why Redis as broker** — In-memory speed, pub/sub support, and Celery's native integration make it ideal for a task queue where latency matters.

---

## Limitations (honest)

This is a learning project. It lacks:

- Authentication on the API
- DAG auto-discovery (tasks are partially hardcoded in worker)
- Distributed scheduler locking (two schedulers would double-trigger)
- In-flight artifact persistence (artifacts lost if orchestrator crashes mid-run)
- Real-time UI with live task progress

---

## What's Next

- [ ] Auto-discovery of `@task` functions across any module
- [ ] `pipekit logs` command with full run history
- [ ] Web UI dashboard with DAG visualization
- [ ] Docker Compose setup for one-command startup
- [ ] Distributed scheduler with Redis lock

---

## Author

Built by **Aditya** as a deep-dive into distributed systems and pipeline orchestration.

*"The biggest thing I learned: reliability in distributed systems comes from state. Without it, a crash means you lose everything."*
