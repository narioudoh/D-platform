FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your project files into the container
COPY processes/ ./processes/
COPY .env .

# Trigger the script (which uses .serve() to stay alive and listen for cron jobs)
CMD ["python", "processes/main.py"]
