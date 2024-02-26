FROM spark

# -- Runtime

ARG hive_thrift_server_port=10000

EXPOSE ${hive_thrift_server_port}

CMD $SPARK_HOME/bin/spark-class org.apache.spark.deploy.SparkSubmit --master spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 spark-internal 