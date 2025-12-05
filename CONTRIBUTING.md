# Contributing to werq

Thank you for your interest in contributing to werq!

## Development Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

1. Install uv if you haven't already:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Clone and set up the project:
   ```bash
   git clone https://github.com/higgcz/werq.git
   cd werq
   uv sync --dev
   ```

3. Install pre-commit hooks:
   ```bash
   uv run pre-commit install
   ```

## Code Style

- We use [ruff](https://github.com/astral-sh/ruff) for linting and formatting
- Type hints are required and checked with [mypy](https://mypy.readthedocs.io/) (strict mode)
- Docstrings follow Google style

Run the linter and formatter:
```bash
uv run ruff check .
uv run ruff format .
```

Run type checking:
```bash
uv run mypy src/werq
```

## Testing

Run the test suite:
```bash
uv run pytest
```

Run with coverage:
```bash
uv run pytest --cov=werq
```

## Pull Request Process

1. Fork the repository and create your branch from `master`
2. Make your changes and add tests if applicable
3. Ensure all tests pass and linting is clean
4. Update documentation if needed
5. Submit a pull request

## Questions?

Feel free to open an issue if you have questions or run into problems.
