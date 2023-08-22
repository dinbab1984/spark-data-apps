//create dataframe from json file
val df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
//schema on read
df.printSchema()
//fetch spark types such as StructType, StructField, StringType, LongType, IntegerType
spark.read.format("json").load("data/flight-data/json/2015-summary.json").schema

//reading json with schema

import org.apache.spark.sql.types.{StructType,StructField,StringType,LongType,Metadata}
val data_schema = StructType(Array(
    StructField("DEST_COUNTRY_NAME",StringType,true),
    StructField("ORIGIN_COUNTRY_NAME",StringType,true),
    StructField("count",LongType,true)
    ))
val df = spark.read.format("json").schema(data_schema).load("data/flight-data/json/2015-summary.json")
df.printSchema()

// Columns, expression (operations)
//referring a column in dataframe
df.col("count")

//scala column construct
import org.apache.spark.sql.functions.{col,column}
col("SampleColumn")
column("SampleColumn")
//scala specific column constructs
$"SampleColumn"
'SampleColumn

//expression , same as Columns
import org.apache.spark.sql.functions.{col,expr}
expr("SampleColumn * 10") == col("SampleColumn") * 10
// to see dataframe Columns
spark.read.format("json").load("data/flight-data/json/2015-summary.json").columns
//dataframe is array of Row Object, command to fetch first row
df.first()

//Collection or Row Object is dataframe
//Creating Row Object
import org.apache.spark.sql.Row
val sample_row=Row("Dinesh","M","test")
sample_row(0)
sample_row.getString(0)

//creating dataframe and register as temp view for sql operations
df.createOrReplaceTempView("dfTempTable")
spark.sql("select * from dfTempTable").show(1)
spark.sql("select * from dfTempTable where ORIGIN_COUNTRY_NAME='India'").show()
spark.sql("select count(*) from dfTempTable").show()

//select and selectExpr
df.select("DEST_COUNTRY_NAME" , "ORIGIN_COUNTRY_NAME").show(5)
// ways to select column in Scala
df.select( col("DEST_COUNTRY_NAME"), column("DEST_COUNTRY_NAME"),df.col("DEST_COUNTRY_NAME")).show(1) 
df.select(expr("DEST_COUNTRY_NAME"), $"DEST_COUNTRY_NAME",'DEST_COUNTRY_NAME).show(1)
//mixing give an error
df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME")
//columns alias with expr
df.select(expr("DEST_COUNTRY_NAME as destination") ,expr( "ORIGIN_COUNTRY_NAME as Origin")).show(5)
df.select(expr("DEST_COUNTRY_NAME as destination").alias("D-COUNTRY")).show(5)
df.select(expr("DEST_COUNTRY_NAME").alias("D-COUNTRY")).show(5)

//selectExpr combines expression and select, to do complex expressions
df.selectExpr("*").show(5)
df.selectExpr("*","(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as WITHIN_COUNTRY").show(5)
//aggregations
df.selectExpr("avg(count) as average_flights", "count(distinct ORIGIN_COUNTRY_NAME) as unique_origins").show()







