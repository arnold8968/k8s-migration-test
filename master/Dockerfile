FROM python:3.6
RUN pip install --upgrade pip && \
    pip install kubernetes numpy pandas && \
    mkdir /data
ADD master.py /root/
WORKDIR /root/