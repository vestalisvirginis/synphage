FROM python:3.10

WORKDIR /usr/src

RUN mkdir -p /dagster
RUN mkdir -p /data/genbank
RUN mkdir -p /data/results

# Create volumes
VOLUME /data
VOLUME /dagster

# NCBI blast standalone
RUN apt update
RUN apt install ncbi-blast+ -y

# Java Runtime
RUN apt install openjdk-17-jdk -y

# Libraries
RUN pip install --upgrade pip
RUN pip install synphage dagster-webserver

# Environment variables 
# To run dagster
ENV DAGSTER_HOME=/dagster
# File config
ENV PHAGY_DIRECTORY=/data
ENV FILE_SYSTEM=$(PHAGY_DIRECTORY)/fs
# NCBI Connect
ENV EMAIL=""
ENV API_KEY=""

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "synphage"]