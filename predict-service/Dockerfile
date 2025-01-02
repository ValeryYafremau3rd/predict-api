FROM python:3.10-slim AS builder
EXPOSE 8087
WORKDIR /predictor-service2
COPY requirements.txt /predictor-service2
RUN pip3 install -r requirements.txt --no-cache-dir
COPY . /predictor-service2
ENTRYPOINT ["python3","-u", "predictor-service.py"]