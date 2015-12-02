FROM ubuntu:latest
MAINTAINER szalai1

RUN apt-get update && apt-get install -y openjdk-7-jdk \
                                         wget \
                                         tar 

RUN wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/zookeeper/zookeeper-3.3.6/zookeeper-3.3.6.tar.gz && \
    tar xzvf zookeeper-3.3.6.tar.gz && \
    rm zookeeper-3.3.6.tar.gz

RUN printf "tickTime=2000\ndataDir=/var/zookeeper\nclientPort=2181\n" > zookeeper-3.3.6/conf/zoo.cfg
RUN mkdir -p /var/zookeeper
EXPOSE 2181 2888 3888
ENTRYPOINT zookeeper-3.3.6/bin/zkServer.sh
CMD ["start-foreground"]


