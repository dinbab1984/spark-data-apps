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

val countryFilter = col("Country").isin("Germany","France")
val priceFilter = col("UnitPrice") < 1
df.where(priceFilter.or(countryFilter)).sort("UnitPrice").show(5)

val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter))
).where("isExpensive"
).select("unitPrice", "isExpensive").show(5)


df.withColumn("isExpensive", not(col("UnitPrice").leq(250))
).filter("isExpensive"
).select("Description", "UnitPrice").show(5)


df.withColumn("isExpensive", expr("NOT UnitPrice <= 250") 
).filter("isExpensive"
).select("Description", "UnitPrice").show(5) //df.where("UnitPrice >250").select("Description", "UnitPrice").show(5)


//Working with numbers
val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
df.selectExpr("CustomerId", "POWER((Quantity * UnitPrice), 2) + 5 as RealQuantity").show(2)

df.select(round(col("UnitPrice"),1),bround(col("UnitPrice"),2)).show(2)
df.selectExpr("round(UnitPrice,1) as round" ," bround(UnitPrice,1) as bround ").show(2)

df.select(round(lit("4.5")),bround(lit("4.5"))).show(2)

df.stat.corr("Quantity","UnitPrice")
df.select(corr("Quantity","UnitPrice")).show()

df.describe().show()


val colName = "UnitPrice"
val quantileProbs = Array(0.5)
val relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(Seq("StockCode", "Quantity")).show()

df.select(monotonically_increasing_id()).show(2)

// Working with Strings
df.select(initcap(col("Description")),col("Description")).show(2, false)

df.select(col("Description"),lower(col("Description")),upper(lower(col("Description")))).show(2)

df.select(ltrim(lit(" HELLO ")).as("ltrim")).show(1)
df.select(rtrim(lit(" HELLO ")).as("rtrim")).show(1)
df.select(trim(lit(" HELLO ")).as("trim")).show(1)
df.select(lpad(lit("HELLO"), 10, " ").as("lp")).show(1)
df.select(rpad(lit("HELLO"), 10, " ").as("rp")).show(1)