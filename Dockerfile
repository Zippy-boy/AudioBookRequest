FROM node:22-alpine AS ui-build
WORKDIR /src

# Build the UI when the frontend workspace is present; otherwise leave an empty
# output directory so backend-only checkouts can still build.
COPY . .
RUN if [ -f ui/package.json ]; then \
      cd ui && \
      if [ -f package-lock.json ]; then npm ci; \
      elif [ -f pnpm-lock.yaml ]; then corepack enable pnpm && pnpm install --frozen-lockfile; \
      elif [ -f yarn.lock ]; then corepack enable yarn && yarn install --frozen-lockfile; \
      else npm install; fi && \
      npm run build && \
      mkdir -p /out/app/static/ui && \
      cp -R dist/. /out/app/static/ui/; \
    else \
      mkdir -p /out/app/static/ui; \
    fi

# ---- Python deps ----
FROM astral/uv:python3.13-alpine AS python-deps
WORKDIR /app
COPY uv.lock pyproject.toml ./
RUN uv sync --frozen --no-cache --no-dev

# ---- Final ----
FROM python:3.13-alpine AS final
WORKDIR /app

COPY --from=python-deps /app/.venv /app/.venv
COPY --from=ui-build /out/app/static/ui /app/app/static/ui

COPY alembic/ alembic/
COPY alembic.ini alembic.ini
COPY app/ app/
COPY CHANGELOG.md CHANGELOG.md

ENV ABR_APP__PORT=8000
ARG VERSION
ENV ABR_APP__VERSION=$VERSION

CMD /app/.venv/bin/alembic upgrade heads && /app/.venv/bin/fastapi run --port $ABR_APP__PORT
