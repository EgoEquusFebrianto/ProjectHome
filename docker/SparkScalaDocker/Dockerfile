FROM apache/spark:3.5.3

WORKDIR /app

COPY target/scala-2.13/SparkScalaDocker.jar /app/

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "SparkDockerApp", "/app/SparkScalaDocker.jar"]