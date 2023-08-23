FROM apache/airflow:2.6.3-python3.9
USER root

ENV SPARK_VERSION=3.4.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

RUN apt update && apt install -y wget

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# install Spark
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN tar xvf spark-*

RUN rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

RUN export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.231/aws-java-sdk-bundle-1.12.231.jar --output ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.231.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar --output ${SPARK_HOME}/jars/hadoop-aws-3.3.1.jar
RUN curl https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar --output ${SPARK_HOME}/jars/jets3t-0.9.4.jar

USER airflow

COPY ./airflow/requirements.txt /
RUN pip install -r /requirements.txt

COPY --chown=airflow:root ./airflow/dags /opt/airflow/dags
COPY --chown=airflow:root ./airflow/jobs /opt/airflow/jobs