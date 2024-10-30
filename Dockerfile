# Use a base Python image
FROM python:3.9-slim

# Create a working directory
WORKDIR /app

# Copy project files into the container
COPY . /app

# Install dependencies, including duckdb
RUN pip install --no-cache-dir -r requirements.txt 
# Command to run the pipeline (adjust if you have a main script)
CMD ["python", "main.py"]
