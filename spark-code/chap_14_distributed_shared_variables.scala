//creating rdd (resilient distributed dataset)
val col1 = "This is a spark collection"
val rdd1 = spark.sparkContext.parallelize(col1.split(" "),2)
//BV - Broadcast variables , pass shared rdd to all executors e.g. lookup table
val rdd_bv =Map("spark" -> 9 , "is" -> 6 )
val broadCast = spark.sparkContext.broadcast(rdd_bv)
broadCast.value // access values
rdd1.map(x => (x , broadCast.value.getOrElse(x,-1))).collect()
//Array((This,-1), (is,6), (a,-1), (spark,9), (collection,-1))

//Accumulators // all excutors sent a value to driver for accumulation
//e.g. counter to driver
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)
val flights = spark.read.parquet("data/flight-data/parquet/2010-summary.parquet").as[Flight]
//define accumlator
import org.apache.spark.util.LongAccumulator // spark typed
val accChina = new LongAccumulator
spark.sparkContext.register(accChina, "China")
//acculmator function 
def accChinaFunc(flight_row: Flight) = {
 val destination = flight_row.DEST_COUNTRY_NAME
 val origin = flight_row.ORIGIN_COUNTRY_NAME
 if (destination == "China") {
  accChina.add(flight_row.count.toLong)
 }
 if (origin == "China") {
  accChina.add(flight_row.count.toLong)
 }
 }
flights.foreach(flight_row => accChinaFunc(flight_row))
accChina.value // 953, 1906 and so on
 // it is also possible to create custom type accumulators e.g. class of somrthing