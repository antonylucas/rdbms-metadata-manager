FROM python:3.10-slim


WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


RUN pip install python-dotenv


COPY . .


RUN mkdir -p exports-sample


ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
