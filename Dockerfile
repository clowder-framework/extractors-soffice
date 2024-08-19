FROM python:3.10

RUN apt-get update && apt-get install -y libreoffice

COPY requirements.txt ./
RUN pip install -r requirements.txt --no-cache-dir

COPY extractor_info.json extractor.py ./

WORKDIR ./
ENV PYTHONPATH=./

CMD ["python3","extractor.py", "--heartbeat", "40"]

