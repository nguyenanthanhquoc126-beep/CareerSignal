FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER 185