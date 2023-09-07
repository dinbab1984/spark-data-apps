//schema for dataset
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
//dataframe to Dataset
val flightsDF = spark.read.parquet("data/flight-data/parquet/2010-summary.parquet/")
val flights = flightsDF.as[Flight]
//actions
flights.show(2)
flights.first.DEST_COUNTRY_NAME
//transformations
//filtering . custom function, same DF functions wokrk anway, this one is just show how custom typed function work in dataset
def originIsDestination(flight_row: Flight): Boolean = {
return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
}
flights.filter(flight_row => originIsDestination(flight_row)).first()
//collect to driver and execute in driver
flights.collect().filter(flight_row => originIsDestination(flight_row))
//mapping e.g. simple data extraction from dataset
val destinations = flights.map(f => f.DEST_COUNTRY_NAME)

//joins
//joinWith
case class FlightMetadata(count: BigInt, randomData: BigInt)
val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong)
    ).withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData"
    ).as[FlightMetadata]
val flights2 = flights.joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))
//datasets are nested as _1 and _2, to access a column
flights2.selectExpr("_1.DEST_COUNTRY_NAME")
//regular join also work but returns DF
val flights2 = flights.join(flightsMeta, Seq("count")) // flat DF not nesting , 
//also convert Dataset to DF thought the result is same
val flights2 = flights.join(flightsMeta.toDF(), Seq("count"))
//Grouping and Aggregations
flights.groupBy("DEST_COUNTRY_NAME").count() //returns DF
flights.groupByKey(x => x.DEST_COUNTRY_NAME).count() //returns Dataset
flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain
//using funtions in aggregation
def grpSum(countryName:String, values: Iterator[Flight]) = {
values.dropWhile(_.count < 5).map(x => (countryName, x))
}
flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5,false)
//dataset are suggested to use at the begining or at the end due to its performance intensiveness
def grpSum2(f:Flight):Integer = {1}
flights.groupByKey(x => x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5)
def sum2(left:Flight, right:Flight) = {
Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
}
flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => sum2(l, r)).take(5)
flights.groupBy("DEST_COUNTRY_NAME").count().explain
