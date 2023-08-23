//create dataframe from json file
val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")
spark.sql("select * from dfTable").show(1)

//convert into spark data types
df.select(lit(10),lit("number"),lit(10.253))

//Boolean expressions
df.where(col("InvoiceNo").equalTo(536365)).select("InvoiceNo", "Description").show(5, false)
df.where("InvoiceNo == 536365").show(5)
df.where("InvoiceNo == 536365").select("InvoiceNo","Description").show(3)


