//create dataframe from json file
val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/retail-data/by-day/*.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")
spark.sql("select * from dfTable").show(1)

//simple aggregations
df.count()
df.select(count("InvoiceNo")).show()
df.select(countDistinct("InvoiceNo")).show()
df.select(approx_count_distinct("InvoiceNo")).show()
df.select(first("InvoiceNo"),last("InvoiceNo")).show()
df.select(min("InvoiceNo"),max("InvoiceNo")).show()
df.select(sum("UnitPrice"),sumDistinct("UnitPrice")).show()
df.select(sum("Quantity"),sumDistinct("Quantity")).show()
df.selectExpr("avg(Quantity)","sum(Quantity)/count(Quantity)","mean(Quantity)").show()
df.selectExpr("variance(UnitPrice)","stddev(UnitPrice)","var_samp(UnitPrice)","stddev_samp(UnitPrice)").show()
df.selectExpr("skewness(UnitPrice)","kurtosis(UnitPrice)").show()
df.selectExpr("covar_pop(UnitPrice,Quantity)","corr(UnitPrice,Quantity)").show()
df.selectExpr("collect_list(Country)","collect_set(Country)").show()
df.agg(collect_list("Country"),collect_set("Country")).show()

//Grouping
df.groupBy("Country").agg(count("InvoiceNo"),expr("count(InvoiceNo)")).show(3)
df.groupBy("Country","StockCode").agg(count("InvoiceNo"),expr("count(InvoiceNo)")).show(3)
df.groupBy("Country","StockCode").agg("UnitPrice" ->"min","UnitPrice" -> "max").show(3)

//window function
import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy(col("CustomerId"),col("InvoiceDate")).orderBy(col("Quantity").desc
    ).rowsBetween(Window.unboundedPreceding, Window.currentRow)

val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

val purchaseDenseRank = dense_rank().over(windowSpec)

val purchaseRank = rank().over(windowSpec)

df.where("CustomerId IS NOT NULL").orderBy("CustomerId"
    ).select(col("CustomerId"),col("InvoiceDate"),col("Quantity"),
    purchaseRank.alias("quantityRank"),purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

    
//rollup
df.drop().select(to_date(col("InvoiceDate"),"").alias("Date"),col("Country"),col("Quantity")
    ).rollup("Date","Country").agg(sum("Quantity")
    ).selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity"
    ).orderBy("Date").show()
//cube
df.drop().select(to_date(col("InvoiceDate"),"yyyy-MM-dd").alias("Date"),col("Country"),col("Quantity")
    ).cube("Date","Country").agg(sum("Quantity")
    ).orderBy("Date").show()
//grouping_id --> level or depth
df.drop().select(to_date(col("InvoiceDate"),"yyyy-MM-dd").alias("Date"),col("Country"),col("Quantity")
    ).cube("Date","Country").agg(grouping_id(),sum("Quantity")
    ).orderBy("Date").show()
//pivot
val df_pivot = df.drop().select(to_date(col("InvoiceDate"),"yyyy-MM-dd").alias("Date"),col("Country"),col("Quantity")
    ).groupBy("Date").pivot("Country").sum()
df_pivot.select("Date","Germany").show(3)

//UDAFs - User Defined Aggregate Functions.



