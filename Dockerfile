# Dockerfile for mock handle server
FROM python:3.11-slim

WORKDIR /app

# Copy only what's needed for installation
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/

# Install the package
RUN pip install --no-cache-dir . flask

EXPOSE 8000

CMD ["python", "src/piddiplatsch/testing/mock_handle_server.py"]
