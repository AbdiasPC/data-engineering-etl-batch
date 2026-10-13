# One image for the Python pipeline and dbt.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DBT_PROFILES_DIR=/app/dbt

RUN useradd --create-home --uid 1000 app

WORKDIR /app

# Dependencies first so code changes do not invalidate this layer.
COPY requirements.txt requirements-dev.txt requirements.lock ./
RUN pip install -r requirements.txt -r requirements-dev.txt -c requirements.lock

COPY --chown=app:app . .

USER app

CMD ["python", "-m", "src.pipeline", "--help"]
