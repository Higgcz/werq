# dirq

A simple, directory-based job queue system.

## Features

- File system based - no external dependencies
- Simple to use and understand
- Perfect for small to medium workloads
- Built-in progress tracking
- Automatic cleanup
- DataFrame-based job listing

## Installation

```bash
pip install dirq
```

## Quick Start

```python
from dirq import JobQueue, Worker
from pathlib import Path

# Initialize queue
queue = JobQueue(Path("jobs"))

# Submit a job
job_id = queue.submit_job({"type": "analysis", "data": [1, 2, 3]})


# Create a worker
class MyWorker(Worker):
    def process_job(self, job_id, params, result_dir):
        # Do work...
        self.queue.update_progress(job_id, 50.0)
        # Save results...


# Run worker
worker = MyWorker(queue)
worker.run()
```

## CLI Usage

```bash
# Submit a job
dirq submit job_params.json --monitor

# List all jobs
dirq list

# Start a worker
dirq worker
```

## Development Setup

This project uses `uv` for dependency management. If you don't have `uv` installed, you can install it with:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

To set up the development environment:

```bash
# Clone the repository
git clone https://github.com/higgcz/dirq.git
cd dirq

# Install dependencies and set up development environment
uv sync --dev

# Install pre-commit hooks
uv run pre-commit install
```

## Development Commands

```bash
# Run tests
uv run pytest

# Run type checking
uv run mypy src/dirq

# Run linter
uv run ruff check .

# Format code
uv run ruff format .

# Build package
uv build

# Sync dependencies (install/update)
uv sync --dev
```

## License

MIT License
