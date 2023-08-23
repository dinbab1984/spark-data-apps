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

//how to use literals for contant values
df.selectExpr("*", "1 as one").show(2)
df.select(expr("*"), expr("1 as one")).show(2)
df.select(expr("*"), lit(1).as("one")).show(2)

//adding columns
df.withColumn("One",expr("1")).show(2)
df.withColumn("One",lit("1")).show(2)
df.withColumn("Route", expr("ORIGIN_COUNTRY_NAME =  DEST_COUNTRY_NAME")).show(2)

//renaming columns
df.withColumnRenamed("ORIGIN_COUNTRY_NAME",  "org country").withColumnRenamed("DEST_COUNTRY_NAME","dest country").show(2)
//(escape)reserved characters e.g. whitespaces
val df_new = df.withColumnRenamed("ORIGIN_COUNTRY_NAME",  "org country").withColumnRenamed("DEST_COUNTRY_NAME","dest country")
df_new.selectExpr("`dest country` as `destination country`","`org country` as `origin country`").show(2)
df_new.select(col("dest country").as("destination country")).show(2) // no escape char needed here
df.select(concat(col("ORIGIN_COUNTRY_NAME"),lit("-"),col("DEST_COUNTRY_NAME"))).show(2)

//case senstivitiy by default not, change config as follow
set spark.sql.caseSensitive true

//drop columns
df_new.drop("count").show(2) //only for show
df_new.drop("count").columns // to create ot overwrite the dataframe

//column type cast
df.select(col("count").cast("long")).show(2)

//filtering row
df.filter(col("count")>5000).show()
df.where("count > 5000").show()
df.where(col("count") > 100).where("count < 120").show()
df.where(col("ORIGIN_COUNTRY_NAME") === "Qatar").where("count < 120").show()

//unique records
df.select("DEST_COUNTRY_NAME" , "ORIGIN_COUNTRY_NAME").distinct().show(2)
df.select("DEST_COUNTRY_NAME" , "ORIGIN_COUNTRY_NAME").distinct().count()

//randon samples
val seed = 5
val withReplacement = false
val fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

//random splits , useful to split dataframe into multiple pieces randomly
val df_splits = df.randomSplit(Array(0.3,0.2,.5))
df_splits(0).show(2)
df_splits(1).show(2)
df_splits(2).show(2)

//concatenating and appending rows
val df_schema = df.schema // copy existing df schema
import org.apache.spark.sql.Row //import Row obj lib
val row = Row("Hello","World",10L) //create a row object
val arr_row=Array(row, row, row,row) // array of row objets
val par_rows = sc.parallelize(arr_row) // parallelize row objects
val new_df = spark.createDataFrame(par_rows,df_schema) //create df from row obj and schema 
new_df.show()//view new df
df.union(new_df).count()) // union count
val combined_df = df.union(new_df) // new df with combined two dfs

//sort rows
df.sort("ORIGIN_COUNTRY_NAME").show(5)
df.orderBy("ORIGIN_COUNTRY_NAME").show(5)
import org.apache.spark.sql.functions.{asc,desc}
df.orderBy(asc("ORIGIN_COUNTRY_NAME")).show(5)
df.orderBy(desc("ORIGIN_COUNTRY_NAME")).show(5)
//limit
df.limit(3).show()
//num of partitions
df.rdd.getNumPartitions
//repartition, schuffle partitions , performance intensive and expensive
df.repartition(5)
df.repartition(col("DEST_COUNTRY_NAME"))
df.repartition(5,col("DEST_COUNTRY_NAME"))
// reduce partitions without reshuffle and no performance impact
 df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2).rdd.getNumPartitions



