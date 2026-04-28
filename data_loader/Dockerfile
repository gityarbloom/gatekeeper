FROM python:3.11-slim

WORKDIR /app/data_loader

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "main.py"]