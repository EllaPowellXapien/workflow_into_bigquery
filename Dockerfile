# Use a lightweight Python base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements first (so Docker cache works efficiently)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything else in your repo
COPY . .

# Run your app
CMD ["python", "main.py"]
