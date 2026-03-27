FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

RUN mkdir -p uploads watch/processed

EXPOSE 5000

ENV PORT=5000

CMD ["python3", "app.py"]
