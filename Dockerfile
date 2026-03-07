FROM python:3.12-slim

# Install FFmpeg and system tools
RUN apt-get update && \
    apt-get install -y ffmpeg curl && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy all your files into the container
COPY . /app

# Install Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Start the API using Gunicorn with a massive 10-minute timeout
CMD ["gunicorn", "-b", "0.0.0.0:10000", "--timeout", "600", "processor_api:app"]
