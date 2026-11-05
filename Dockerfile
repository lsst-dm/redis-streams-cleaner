FROM python:3.14.8-slim-trixie

ENV PYTHONUNBUFFERED=True

WORKDIR /app

# Copy pyproject.toml and source
COPY pyproject.toml README.md ./
COPY src ./src

# Install package and dependencies from pyproject.toml
# This installs redis + any other dependencies listed
RUN pip install --no-cache-dir .

# Set Python path
ENV PYTHONPATH=/app/src

# Run
CMD ["python", "-m", "redis_streams_cleaner.main"]