FROM apache/airflow:2.10.2-python3.8

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
