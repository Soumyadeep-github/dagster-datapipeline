FROM python:3.10-slim

WORKDIR /app
ENV DAGSTER_HOME=/app

COPY requirements.txt /app/requirements.txt
EXPOSE 3000
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/
# COPY dagster.yaml workspace.yaml /app/
# COPY pipelines /app/pipelines

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
