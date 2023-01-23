FROM python:3.10.8

RUN apt update -y
RUN apt install wget -y

WORKDIR /usr/src

COPY requirements.txt .

RUN apt install openjdk-11-jdk -y
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN mkdir -p /usr/src/data
VOLUME /usr/src/data