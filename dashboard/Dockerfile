FROM python:3.12-slim

WORKDIR /app

# System deps (optional: only if you need build tools for packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# Install python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
# Copy src and outputs
COPY src/ src/
COPY outputs/ outputs/
# Copy dashboard code into a dashboard subdirectory
COPY dashboard/ dashboard/

# Flask should listen on all interfaces in container
ENV PYTHONUNBUFFERED=1
ENV PORT=5001
ENV BASE_DIR=.

EXPOSE 5001

# Start command (adjust the path to your entry file)
CMD ["python", "dashboard/flask_app.py"]