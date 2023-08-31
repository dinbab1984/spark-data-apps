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
////SQL Lite
////Read from database table
val driver = "org.sqlite.JDBC"
val path = "/data/flight-data/jdbc/my-sqlite.db"
val url = s"jdbc:sqlite:/${path}"
val tablename = "flight_info"
//connection test
import java.sql.DriverManager
val connection = DriverManager.getConnection("jdbc:sqlite:/data/flight-data/jdbc/my-sqlite.db")
connection.isClosed()
connection.close()

val dbDataFrame = spark.read.format("jdbc").option("url", "jdbc:sqlite:/data/flight-data/jdbc/my-sqlite.db"
).option("dbtable", "flight_info").option("driver", "org.sqlite.JDBC").load()