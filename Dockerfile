FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8282

CMD gunicorn --bind 0.0.0.0:$PORT --workers 2 --timeout 60 app:app
