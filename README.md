# spark-data-apps

#### You can easily run this locally on your local docker engine, please follow the steps below:

1. Clone this repository locally
2. Install docker locally
3. Start docker engine locally
4. Go to the cloned repository directory i.e. Spark-data-apps
5. Run docker build (spark) as follows : ````docker build -f spark.Dockerfile -t spark .````
6. Run docker build (master) as follows : ````docker build -f spark-master.Dockerfile -t spark-master .````
7. Run docker build (worker) as follows : ````docker build -f spark-worker.Dockerfile -t spark-worker .````
8. Run docker build (hive-thriftserver) as follows : ````docker build -f hive-thriftserver.Dockerfile -t hive-thriftserver .````
8. Run docker as follows: ````docker-compose up````
7. Check if the master web UI opens : http://localhost:8080
9. Check if the worker1 web UI open: http://localhost:8081
10. Check if the worker2 web UI open: http://localhost:8082




