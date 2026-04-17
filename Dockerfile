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

# Create directories for temp files and persistent data
RUN mkdir -p /tmp/kindle_files /app/data/books

# Persistent data volume
VOLUME ["/app/data"]

# Health check (bot exposes port 8085)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8085/health')" || exit 1

EXPOSE 8085 8086

CMD ["python", "-u", "bot.py"]
