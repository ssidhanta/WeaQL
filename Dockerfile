#FROM ubuntu:16.04
FROM openjdk:8-jdk-alpine
#FROM anapsix/alpine-java
RUN mkdir /usr/local/zookeeper
RUN mkdir /usr/local/zookeeper/data
RUN mkdir /usr/local/zookeeper/log
RUN mkdir /var/lib/thesis_code-master
ENV PATH="/bin/sh:/bin/bash:${PATH}"
COPY resources/topologies/docker_tpcc_7node.xml /var/lib/thesis_code-master/
COPY resources/log4j_zookeeper.properties /var/lib/thesis_code-master/
COPY resources/zoo.cfg /var/lib/thesis_code-master/
COPY dist/jars/zookeeper-server.jar /var/lib/thesis_code-master/
COPY src/weaql/server/agents/coordination/zookeeper/EZKCoordinationExtension.java /var/lib/thesis_code-master/src/weaql/server/agents/coordination/zookeeper/
RUN chmod -R 777 /usr/local/zookeeper
RUN chmod -R 777 /var/lib/thesis_code-master
CMD java -Dlog4j.configuration=file:"/var/lib/thesis_code-master/log4j_zookeeper.properties" -jar /var/lib/thesis_code-master/zookeeper-server.jar /var/lib/thesis_code-master/zoo.cfg && tail -f /dev/null
