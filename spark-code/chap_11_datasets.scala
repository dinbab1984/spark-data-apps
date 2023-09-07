//SQL CLI
command --> spark-SQL
//Thrift JDBC/ODBC Server
//start server
//windows
spark-class org.apache.spark.deploy.SparkSubmit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 spark-internal
//linux 
./sbin/start-thriftserver.sh
//connect command line --> beeline
beeline
jdbc:hive2://localhost:10000

//start spark-sql
spark-sql --conf spark.sql.catalogImplementation=hive

//create spark managed tables (metadata and data)
CREATE TABLE flights (
DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
USING JSON OPTIONS (path '/data/flight-data/json/2015-summary.json');
//create table from another tables (also check if table already exists)
CREATE TABLE IF NOT EXISTS flights_from_select
AS SELECT * FROM flights;
//create paritioned table
CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5

//creating external tables(unmanaged table only metadata, data stored externally)
CREATE EXTERNAL TABLE hive_flights (
DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/flight-data-hive/';
//create table from another tables (also check if table already exists)
CREATE EXTERNAL TABLE hive_flights_2
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/flight-data-hive/' AS SELECT * FROM flights

//insert data into tables (managed tables)
INSERT INTO flights_from_select
SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20;
//insert data into partitioned tables
INSERT INTO partitioned_flights
PARTITION (DEST_COUNTRY_NAME="UNITED STATES")
SELECT count, ORIGIN_COUNTRY_NAME FROM flights
WHERE DEST_COUNTRY_NAME='UNITED STATES' LIMIT 12;

//describe table defintion
DESCRIBE TABLE flights;
//describe partitioned tables
SHOW PARTITIONS partitioned_flights

//refresh table metadata
REFRESH table partitioned_flights
MSCK REPAIR TABLE partitioned_flights

//drop table
DROP TABLE flights; // data deleted only for managed tables , external tables only the metadata
//if exists
DROP TABLE IF EXISTS flights;

//cache and uncache table
CACHE TABLE flights;
UNCACHE TABLE flights;

//Views
CREATE VIEW just_usa_view AS
SELECT * FROM flights WHERE dest_country_name = 'United States';
//temp view , automatically drop after seesion ends
CREATE TEMP VIEW just_usa_view_temp AS
SELECT * FROM flights WHERE dest_country_name = 'United States';
//global temp view, available regardless of database and available across spark application
CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS
SELECT * FROM flights WHERE dest_country_name = 'United States';4
//replace view
CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
SELECT * FROM flights WHERE dest_country_name = 'United States'

//show tables, databases
SHOW DATABASES;
SHOW TABLES;
//explain query plan
EXPLAIN SELECT * FROM just_usa_view;

//drop views
DROP VIEW IF EXISTS just_usa_view;

//create database
CREATE DATABASE some_db;
//set current database
USE some_db;
//drop database
DROP DATABASE IF EXISTS some_db;

//case when then statement
SELECT
CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1
WHEN DEST_COUNTRY_NAME = 'Egypt' THEN 0
ELSE -1 END
FROM partitioned_flights;

//Complex Types
//Structs
CREATE VIEW IF NOT EXISTS nested_data AS
SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights;
SELECT * FROM nested_data;
SELECT country.DEST_COUNTRY_NAME, count FROM nested_data;
SELECT country.*, count FROM nested_data;

//Lists
//collect list array of all, collect set array of unique values
SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
collect_set(ORIGIN_COUNTRY_NAME) as origin_set
FROM flights GROUP BY DEST_COUNTRY_NAME;
SELECT DEST_COUNTRY_NAME, ARRAY(1, 2, 3) FROM flights;
SELECT DEST_COUNTRY_NAME as new_name, collect_list(count)[0]
FROM flights GROUP BY DEST_COUNTRY_NAME;
//array back to rows
CREATE OR REPLACE TEMP VIEW flights_agg AS
SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts
FROM flights GROUP BY DEST_COUNTRY_NAME;
SELECT explode(collected_counts), DEST_COUNTRY_NAME FROM flights_agg;
//functions
SHOW FUNCTIONS;
SHOW SYSTEM FUNCTIONS;
SHOW USER FUNCTIONS;
SHOW FUNCTIONS "s*";
SHOW FUNCTIONS LIKE "collect*";
//define and registet user defined functions in spark-shell
def power3(number:Double):Double = number * number * number
spark.udf.register("power3", power3(_:Double):Double) //You can also register functions through the Hive CREATE TEMPORARY FUNCTION syntax.
SELECT count, power3(count) FROM flights;
//sub queries
//uncorrelated
SELECT dest_country_name FROM flights
GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5;
SELECT * FROM flights
WHERE origin_country_name IN (SELECT dest_country_name FROM flights
GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5);
//correlated
SELECT * FROM flights f1
WHERE EXISTS (SELECT 1 FROM flights f2
WHERE f1.dest_country_name = f2.origin_country_name)
AND EXISTS (SELECT 1 FROM flights f2
WHERE f2.dest_country_name = f1.origin_country_name);
//uncorrelated scalar 
SELECT *, (SELECT max(count) FROM flights) AS maximum FROM flights;





