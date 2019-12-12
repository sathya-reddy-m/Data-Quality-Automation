FROM ubuntu:19.04

#-------------------------------------------------------------------------------
# Install dependencies
#-------------------------------------------------------------------------------
RUN apt update -y && apt upgrade -y

#-------------------------------------------------------------------------------
# Install Java
#-------------------------------------------------------------------------------
RUN apt-get install -y openjdk-8-jdk openjdk-8-jre curl gnupg

#-------------------------------------------------------------------------------
# Install Spark
#-------------------------------------------------------------------------------
ARG SPARK_VERSION=2.4.3
ARG HADOOP_VERSION=2.7
ENV SPARK_HOME=/usr/spark
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$SPARK_HOME/python/lib/
ENV PYSPARK_PYTHON=/usr/bin/python


RUN curl -L --retry 3 \
    "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | \
    gunzip | tar x -C /usr/ && \
    ln -s /usr/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME && \
    rm -rf $SPARK_HOME/examples

#-------------------------------------------------------------------------------
# Install SBT
#-------------------------------------------------------------------------------

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt update && \
    apt install -y sbt

#-------------------------------------------------------------------------------
# Install Additional Libraries
#-------------------------------------------------------------------------------
RUN curl https://repo1.maven.org/maven2/com/amazon/deequ/deequ/1.0.2/deequ-1.0.2.jar -- output /usr/spark-2.4.3-bin-hadoop2.7/jars/deequ-1.0.2.jar && \
    curl https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/3.1.0/azure-storage-3.1.0.jar --output /usr/spark-2.4.3-bin-hadoop2.7/jars/azure-storage-3.1.0.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.3/hadoop-azure-2.7.3.jar --output /usr/spark-2.4.3-bin-hadoop2.7/jars/hadoop-azure-2.7.3.jar && \
    curl https://repo1.maven.org/maven2/org/rogach/scallop_2.11/3.3.1/scallop_2.11-3.3.1.jar --output /usr/spark-2.4.3-bin-hadoop2.7/jars/scallop_2.11-3.3.1.jar
WORKDIR /app    
