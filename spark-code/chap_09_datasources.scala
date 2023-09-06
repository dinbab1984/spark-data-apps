//Reading CSV file
import org.apache.spark.sql.types.{StructType,StructField,StringType,LongType}
val csv_src_schema = StructType(Array(
    StructField("DEST_COUNTRY_NAME",StringType,true),
    StructField("ORIGIN_COUNTRY_NAME",StringType,true),
    StructField("count",LongType,true)
    ))
////infer schema 
val df_csv_src = spark.read.format(
    "csv"
    ).option("header","true"
    ).option("mode","FAILFAST"
    ).option("path","data/flight-data/csv/2015-summary.csv"
    ).option("inferSchema","true"
    ).load(
    ).show(2,false)
////explicit schema
val df_csv_src = spark.read.format(
    "csv"
    ).option("header","true"
    ).option("mode","FAILFAST"
    ).option("path","data/flight-data/csv/2015-summary.csv"
    ).schema(csv_src_schema
    ).load(
    ).show(3,false)
////incorrect explicit schema
val csv_src_schema_x = StructType(Array(
    StructField("DEST_COUNTRY_NAME",LongType,true),
    StructField("ORIGIN_COUNTRY_NAME",StringType,true),
    StructField("count",LongType,true)
    ))
val df_csv_src_x = spark.read.format(
    "csv"
    ).option("header","true"
    ).option("mode","FAILFAST"
    ).option("path","data/flight-data/csv/2015-summary.csv"
    ).schema(csv_src_schema_x
    ).load(
    ).show(3,false)
//writing CSV file
df_csv_src.write.format("csv"
    ).option("header","true"
    ).option("sep","\t"
    ).mode("overwrite"
    ).option("path","tmp/2015-summary.tsv"
    ).save()
//Reading JSON file
////multiline json is allowed in spark, but singleline json is  recommended 
spark.read.format("json").option("mode", "FAILFAST").schema(csv_srv_schema
    ).load("data/flight-data/json/2010-summary.json").show(5)
//Writing JSON file
df_csv_src.write.format("json").mode("overwrite").save("tmp/2015-summary.json")

//Reading Praquet file
spark.read.format("parquet").load("data/flight-data/parquet/2010-summary.parquet").show(5)
//Writing Praquet file
df_csv_src.write.format("parquet").mode("overwrite").save("tmp/2010-summary.parquet")

//Reading ORC file
spark.read.format("orc").load("data/flight-data/orc/2010-summary.orc").show(5)
//Writing ORC file
df_csv_src.write.format("orc").save("tmp/flight-data-2010-summary.orc")

//Reading and writing to SQL Databases
////postgres
////Read from database table
val dbDataFrame = spark.read.format("jdbc").option(
    "url", "jdbc:postgresql://localhost:5432/sample-db"
    ).option("dbtable", "flight_info"
    ).option("user","postgres").option("password","Password123"
    ).load()
////Query pushdowns
dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain()
dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain
////pass query in place of table name
val dbDataFrame = spark.read.format("jdbc").option(
    "url", "jdbc:postgresql://localhost:5432/sample-db"
    ).option("dbtable", """(SELECT DISTINCT "DEST_COUNTRY_NAME" FROM flight_info) as flight_info"""
    ).option("user","postgres").option("password","Password123"
    ).load()
////reading in parallel (partitions)
val dbDataFrame = spark.read.format("jdbc").option(
    "url", "jdbc:postgresql://localhost:5432/sample-db"
    ).option("dbtable", "flight_info"
    ).option("user","postgres").option("password","Password123"
    ).option("numPartitions",10).load()
//predicates to force and split the data into partitions by condition for data processing optimizaton
//// another API jdbc
val conn_props = new java.util.Properties
conn_props.setProperty("user", "postgres")
conn_props.setProperty("password", "Password123")
val predicates = Array(""""DEST_COUNTRY_NAME" = 'India' OR "ORIGIN_COUNTRY_NAME" = 'India'""",
    """"DEST_COUNTRY_NAME" = 'Germany' OR "ORIGIN_COUNTRY_NAME" = 'Germany'""")
spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info",predicates,conn_props)
spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info",predicates,conn_props).show()
spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info",predicates,conn_props).rdd.getNumPartitions
spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info",predicates,conn_props).explain()
////Cautions while using predicates, if the condition true for more than one then duplicate rows across paritions
val predicates = Array(""""DEST_COUNTRY_NAME" != 'India' OR "ORIGIN_COUNTRY_NAME" != 'India'""",
    """"DEST_COUNTRY_NAME" != 'Germany' OR "ORIGIN_COUNTRY_NAME" != 'Germany'""")
spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info",predicates,conn_props).count()//512 instead of 256
//distribute data using sliding widows e.g. evenly into 10 partitions with col val boundaries (low 10 , high 1000000) 
spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db"
    ,"flight_info","COUNT",0L,1000000L,10
    ,conn_props).rdd.getNumPartitions
//write to SQL database tables
//modes e.g. overwrite, append
dbDataFrame.write.mode("append").jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info_test",conn_props)
spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info_test",conn_props).count()

//Advanced I/O 
////writing into partitions
////fixed number of partitions
val dbDataFrame = spark.read.jdbc("jdbc:postgresql://localhost:5432/sample-db","flight_info",conn_props)
dbDataFrame.repartition(5).write.format("csv").save("tmp/flight-info/csv/paritions")
////partition by column
dbDataFrame.write.format("parquet").mode("overwrite").partitionBy("DEST_COUNTRY_NAME").save("tmp/flight-info/csv/partitionBy")
////buckets (same bucket id load into same partitions) - supported only for Spark-managed tables
dbDataFrame.write.format("parquet").mode("overwrite").bucketBy(10, "count").saveAsTable("bucketedFiles")
////Working with Complex Types
/////For instance, CSV files do not support complex types, whereas Parquet and ORC do.
////Managing File Size
dbDataFrame.write.format("csv").mode("overwrite").option("maxRecordsPerFile", 64).save("tmp/flight-info/csv/64recordsperfile")

