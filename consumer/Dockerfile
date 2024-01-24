FROM python:3.8

WORKDIR /app

COPY . /app

RUN python -m venv venv
ENV PATH="/app/venv/bin:$PATH"
RUN /bin/bash -c "source /app/venv/bin/activate"

RUN pip install kafka-python redis


CMD ["python", "Consumer_Agv_json_var.py"]
