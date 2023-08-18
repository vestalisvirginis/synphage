FROM python:3.10.12

RUN apt update -y
RUN apt install wget -y
RUN apt install tar -y
RUN apt install ncbi-blast+ -y
RUN apt install x11-xserver-utils -y

WORKDIR /usr/src

COPY requirements.txt .

RUN apt install openjdk-11-jdk -y
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENV DAGSTER_HOME=/usr/src/dagster_home/

RUN mkdir -p /usr/src/data
VOLUME /usr/src/data