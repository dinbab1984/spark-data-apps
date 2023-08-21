FROM spark

# -- Runtime

ARG spark_worker_web_port=8081

EXPOSE ${spark_worker_web_port}

CMD ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} 
#>> logs/spark-worker.out
#CMD ${SPARK_HOME}/sbin/start-worker.sh spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}
#>> logs/spark-worker.out