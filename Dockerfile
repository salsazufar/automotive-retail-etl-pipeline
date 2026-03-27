FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/dagster/app

COPY requirements.txt /opt/dagster/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/dagster/app/requirements.txt

COPY src /opt/dagster/app/src
COPY dagster_home /opt/dagster/dagster_home

ENV DAGSTER_HOME=/opt/dagster/dagster_home

EXPOSE 4000
