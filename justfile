# https://just.systems

alias d := dev
alias m := migrate
alias cr := create_revision

migrate:
    uv run alembic upgrade heads

create_revision *MESSAGE:
    uv run alembic revision --autogenerate -m "{{MESSAGE}}"

dev: migrate
    uv run fastapi dev

# update all uv packages
upgrade:
    uvx uv-upgrade

types:
    uv run basedpyright
    uv run ruff format --check app
    uv run alembic check
