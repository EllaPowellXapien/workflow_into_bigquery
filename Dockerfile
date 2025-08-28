# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy everything into the container
COPY . /app

# Install dependencies (if requirements.txt exists)
RUN pip install --no-cache-dir -r requirements.txt || true

# Default command (adjust this to your main Python script)
CMD ["python", "try_new_updates.py"]
