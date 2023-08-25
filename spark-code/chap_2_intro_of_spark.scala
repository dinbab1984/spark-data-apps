//read csv into dataframe
val df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("data/flight-data/csv/2015-summary.csv")

//create dfTempView to query using Spark SQL
df.createOrReplaceTempView("dfTempView")
df.show(2)
spark.sql("SELECT * from dfTempView LIMIT 2").show()

//narrow and wide transformations and explain physical plan
df.sort("count").explain() 
// lazy execution 
/// narrow (independent operation within executors + partition). in this read file is narrow 
//  wide (dependent on all executors + partition) , in this case sort is wide
spark.sql("SELECT * from dfTempView ORDER BY count").explain() // creates same plan (this and above code)

//partitions and shuffle partitions
df.rdd.getNumPartitions //spark default partitions allocated
spark.conf.set("spark.sql.shuffle_partitions","5")

//aggregation and order by
//e.g. top 5 destination
spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) from dfTempView GROUP BY DEST_COUNTRY_NAME ORDER BY sum(count) DESC LIMIT 5").show()
df.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)","DEST_TOTAL").sort(desc("DEST_TOTAL")).show(5)
// NOTE: explain will show same pphysical plan