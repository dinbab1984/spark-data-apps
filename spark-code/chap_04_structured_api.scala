//scala

//first dataset with one columnm and covert into dataframe
val df = spark.range(500).toDF("number")
df.select(df.col("number") + 10)
df.collect()


//spark types
import org.apache.spark.sql.types._
val b = ByteType
