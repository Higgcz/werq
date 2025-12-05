# werq

A simple, directory-based job queue system for Python.

## Why werq?

I wanted a job queue that's easy to observe and debug. With werq, everything is just JSON files in directories - you can browse jobs with `ls`, inspect them with `cat`, and understand the system state at a glance.

By relying on filesystem atomicity, werq can scale to multi-machine setups with just a network filesystem. No Redis, no RabbitMQ, no external services.

For more advanced use cases, check out [Celery](https://docs.celeryq.dev/), [Huey](https://huey.readthedocs.io/), or [Dask](https://www.dask.org/). werq is for when you want simplicity - jobs as plain config files, workers that decide how to process them, and each job getting its own result directory.

## Features

- **Filesystem-Based**: No databases or external daemons required
- **Simple Job Lifecycle**: Jobs progress through `queued` → `running` → `completed` / `failed`
- **Progress Tracking**: Workers can report job progress
- **Built-in Shell Worker**: Execute any shell command
- **Rich CLI**: Modern terminal interface with colored tables

## Installation

```bash
pip install werq
```

## CLI Usage

The CLI operates on a job directory, which defaults to `./jobs`. You can specify a different directory with `--jobs-dir`.

### `submit`

Submit a shell command to be executed by a worker:

```bash
# Submit a simple command
werq submit echo "Hello, World!"

# Submit with a name
werq submit --name data-processing python process.py

# Submit and monitor progress
werq submit --monitor sleep 10
```

### `list`

List all jobs in the queue:

```bash
werq list

# Limit to recent jobs
werq list -n 3
```

Example output:

```
┌──────────────────────┬──────────┬───────────┬─────────────────────┐
│ id                   │ name     │ state     │ created_at          │
├──────────────────────┼──────────┼───────────┼─────────────────────┤
│ 1733312847123456000  │ ingest   │ COMPLETED │ 2024-12-04 10:00:47 │
│ 1733312841987654000  │          │ RUNNING   │ 2024-12-04 10:00:41 │
│ 1733312827891234000  │ backup   │ QUEUED    │ 2024-12-04 10:00:27 │
└──────────────────────┴──────────┴───────────┴─────────────────────┘
```

### `worker`

Start a worker to process jobs:

```bash
# Start a long-running worker
werq worker

# Exit after queue is empty
werq worker --rm

# Custom poll interval
werq worker --poll-interval 5
```

### `info`

Show detailed information about a job:

```bash
werq info <JOB_ID>
```

### `resubmit`

Resubmit a completed or failed job:

```bash
werq resubmit <JOB_ID>

# With a new name
werq resubmit <JOB_ID> --name retry-upload
```

### `rm`

Delete a job:

```bash
werq rm <JOB_ID>
```

### Typical Workflow

1. `werq submit echo "process data"` - enqueue jobs
2. `werq list` - monitor queue
3. `werq worker --rm` - process jobs (exits when done)
4. `werq info <JOB_ID>` - inspect results
5. `werq resubmit <JOB_ID>` - retry failed jobs

## Directory Structure

```
jobs/
├── queued/              # Jobs waiting to be processed
│   └── <job_id>.json
├── running/             # Jobs currently being processed
│   └── <job_id>.json
├── completed/           # Successfully completed jobs
│   └── <job_id>.json
├── failed/              # Failed jobs
│   └── <job_id>.json
└── completed_results/   # Output artifacts
    └── <job_id>/
        ├── result.json  # Job results
        └── error.txt    # Error message (if failed)
```

## Python API

The real power of werq is in the Python API. You can embed job submission in any application - a web dashboard, a script, a notebook - and create custom workers that process jobs however you need.

**Submitting jobs** (e.g., from a web dashboard):

```python
from werq import JobQueue
from pathlib import Path

queue = JobQueue(Path("jobs"))

# Jobs are just dictionaries - define whatever parameters you need
job = queue.submit({
    "type": "optimization",
    "dataset": "experiment_42",
    "learning_rate": 0.001,
    "epochs": 100,
})
print(f"Submitted: {job.id}")
```

**Creating a custom worker**:

```python
from werq import JobQueue, Worker

class OptimizationWorker(Worker):
    def process_job(self, job, *, result_dir):
        params = job.params

        for epoch in range(params["epochs"]):
            # Run your optimization...
            loss = train_epoch(params)

            # Update progress (visible to monitoring)
            self.queue.update_progress(job.id, epoch / params["epochs"] * 100)

            # Save intermediate results to the job's result directory
            (result_dir / f"checkpoint_{epoch}.pt").write_bytes(...)

        # Return final results (saved as result.json)
        return {"final_loss": loss, "model_path": "checkpoint_99.pt"}

# Run the worker
queue = JobQueue(Path("jobs"))
worker = OptimizationWorker(queue)
worker.run()
```

This pattern works great for dashboards and web apps - submit jobs from your frontend, run workers as background processes, and monitor progress in real-time.

## Development

```bash
git clone https://github.com/higgcz/werq.git
cd werq
uv sync --dev
```

## License

MIT License
