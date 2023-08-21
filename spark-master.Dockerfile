FROM spark

# -- Runtime

ARG spark_master_web_port=8080

EXPOSE ${spark_master_web_port} ${SPARK_MASTER_PORT}
CMD ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master