FROM python:3.12-slim

# Install Calibre and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    calibre \
    libgl1 \
    libglib2.0-0 \
    libegl1 \
    libxkbcommon0 \
    libdbus-1-3 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

# Create temp directory for Kindle files
RUN mkdir -p /tmp/kindle_files

CMD ["python", "-u", "bot.py"]
