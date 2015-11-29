FROM ubuntu:latest
MAINTAINER szalai1

RUN apt-get update && apt-get install -y openjdk-7-jdk \
                                         python2.7 \
                                         wget \
                                         tar 

RUN wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/storm/apache-storm-0.10.0/apache-storm-0.10.0.tar.gz && \
    tar xzvf apache-storm-0.10.0.tar.gz && \
    rm  apache-storm-0.10.0.tar.gz && \
    mkdir /mnt/storm
COPY storm.yaml apache-storm-0.10.0/conf/storm.yaml


EXPOSE 8080
ENTRYPOINT apache-storm-0.10.0/bin/storm ui