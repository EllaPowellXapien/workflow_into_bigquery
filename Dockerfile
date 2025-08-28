# Use a lightweight Python base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy everything in your repo into the container
COPY . /app

# Install Python dependencies if requirements.txt exists
RUN pip install --no-cache-dir -r requirements.txt || true

# Run main.py as the entry point when the container starts
CMD ["python", "main.py"]

