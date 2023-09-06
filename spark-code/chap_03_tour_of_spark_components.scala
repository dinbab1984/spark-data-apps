//spark-submit
d:\spark\spark-3.4.1-bin-hadoop3\bin\spark-submit 
--class org.apache.spark.examples.SparkPi 
--master spark://localhost:7077 
d:\spark\spark-3.4.1-bin-hadoop3\examples\jars\spark-examples_2.12-3.4.1.jar

//datasets
//option 1 : using scala case class
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
val flights=spark.read.format("parquet"
    ).option("header","true"
    ).load("data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
val df_flights = flights.as[Flight]
df_flights.show(3)

//option 2: interSchema
val df_flights=spark.read.format("parquet"
    ).option("inferSchema","true"
    ).option("header","true"
    ).load("data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")

//dataset manipulation
df_flights.filter(
    flight => flight.ORIGIN_COUNTRY_NAME == "India"
    ).map(
    flight => flight
    ).show(5)
//window funtion
val df_invoices = spark.read.format("csv"
    ).option("inferSchema","true"
    ).option("header","true"
    ).load("data/retail-data/by-day/*.csv")
val df_invoices_schema = df_invoices.schema
df_invoices.select(
    expr("CustomerID"),
    expr("InvoiceDate"),
    expr("Quantity * UnitPrice as TotalPrice")
    ).groupBy(col("CustomerID"),window(col("InvoiceDate"),"1 day")
    ).sum("TotalPrice").show(5)

//Structured Streaming
val df_invoices_streaming = spark.readStream.schema(df_invoices_schema
    ).option("header","true"
    ).option("maxFilesPerTrigger", 1
    ).format("csv"
    ).load("data/retail-data/by-day/*.csv")

val df_invoices_streaming_cust_totalpurchase_per_Day = df_invoices_streaming.select(
    expr("CustomerID"
    ),expr("InvoiceDate"
    ),expr("Quantity * UnitPrice as TotalPrice"
    )).groupBy(col("CustomerID"),window(col("InvoiceDate"),"1 day")
    ).sum("TotalPrice")

df_invoices_streaming_cust_totalpurchase_per_Day.writeStream.format("console" // memory - to query like from streaming platform
    ).queryName("customer_totalpurchase_per_day"
    ).outputMode("complete"
    ).start()