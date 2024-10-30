# Use a Python base image
FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file and source code to the container
COPY requirements.txt .
COPY src/ ./src/
COPY AccessDW.py ./
COPY README.md ./
COPY logs/ ./logs/
COPY data/ ./data/

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Define the default command to run your application
CMD ["python", "src/main_pipeline.py"]
