FROM python:3.10-slim

WORKDIR /app

RUN pip install fastapi uvicorn pydantic httpx

COPY . .

CMD ["python", "mock_server.py"]
