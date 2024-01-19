FROM python:3.8

WORKDIR /app

COPY . /app

RUN pip install kafka-python


CMD ["python", "Consumer_Agv_json_var.py"]
