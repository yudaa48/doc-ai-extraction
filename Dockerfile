# Use an official Python runtime as a parent image
FROM --platform=linux/amd64 python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    poppler-utils \
    build-essential \
    libpoppler-cpp-dev \
    pkg-config \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Copy requirements first to leverage Docker cache
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_PORT=8080

# Create a non-root user and switch to it
RUN useradd -m -u 1000 appuser

# Ensure the app directory is owned by appuser
RUN chown -R appuser:appuser /app

USER appuser

# Expose the port the app runs on
EXPOSE 8080

# Run the application
CMD ["streamlit", "run", "main.py", "--server.port", "8080", "--server.address", "0.0.0.0"]