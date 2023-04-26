cd /spark/jars
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.0/hadoop-aws-3.3.0.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.2/hadoop-aws-3.2.2.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.384/aws-java-sdk-1.12.384.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.384/aws-java-sdk-bundle-1.12.384.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar
# wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.3.0/mongo-spark-connector_2.12-3.3.0.jar
rm /spark/jars/guava*.jar
wget https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar
cd /
/bin/bash /master.sh