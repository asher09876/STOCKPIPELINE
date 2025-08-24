FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends         gcc         build-essential         libpq-dev         && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1         PYTHONUNBUFFERED=1

COPY dagster_project/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY dagster_project /app/dagster_project

ENV DAGSTER_HOME=/opt/dagster
RUN mkdir -p $DAGSTER_HOME && chmod -R 777 $DAGSTER_HOME

CMD ["bash", "-lc", "python -c 'import dagster; print(dagster.__version__)'"]
