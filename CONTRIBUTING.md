# Contributing

This is a portfolio project, developed openly but maintained by a single author.
External contributions are welcome for bug reports and documentation fixes.

## Development workflow

1. Fork and clone the repository.
2. Copy `.env.example` to `.env` and adjust values if needed.
3. Run `make setup` to install dependencies.
4. Run `make start` to bring up local services (PostgreSQL, Airflow).
5. Run `make test` before opening a pull request.
6. Run `make lint` to check code style (Ruff).

## Commit convention

This repository follows [Conventional Commits](https://www.conventionalcommits.org/):
`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`.

## Code of conduct

Be respectful and constructive. No confidential, proprietary, or personal data
should ever be added to this repository — synthetic or public data only.
