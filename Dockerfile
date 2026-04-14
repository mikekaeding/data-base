FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:${PATH}"

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

RUN useradd --create-home --shell /usr/sbin/nologin appuser

USER appuser

ENTRYPOINT ["flash-dataset"]
CMD ["run-daily", "--working-directory=/data/working", "--storage-directory=/data/storage", "--run-at=6:00AM", "--max-days-back=2"]
