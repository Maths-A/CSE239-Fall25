FROM python:3.9-alpine3.22

WORKDIR /app

# Install dependencies
RUN pip install rpyc

# Copy worker script
COPY worker.py /app/

# Expose the RPC port
EXPOSE 18861

# Run the worker
CMD ["python", "worker.py"]