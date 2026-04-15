FROM python:3.11-slim

WORKDIR /app

# The client needs the ocpp library + websockets
RUN pip install --no-cache-dir ocpp websockets

COPY client.py .

ENV PYTHONUNBUFFERED=1

# No EXPOSE needed as the client initiates the connection
CMD ["python", "client.py"]
