//creating rdd (resilient distributed dataset)
val col1 = "This is a spark collection"
val rdd1 = spark.sparkContext.parallelize(col1.split(" "),2)
//PairRDD
rdd1.map( x => (x.toLowerCase , 1)).collect()//Array((t,1), (i,1), (a,1), (s,1), (c,1))
rdd1.keyBy(x => x.toLowerCase.toSeq(0).toString).collect()//Array((t,This), (i,is), (a,a), (s,spark), (c,collection))
//Manipulation 
////mapValues
val rdd2 = rdd1.keyBy(x => x.toLowerCase.toSeq(0).toString)
rdd2.mapValues(x => x.toUpperCase).collect()//Array((t,THIS), (i,IS), (a,A), (s,SPARK), (c,COLLECTION))
rdd2.flatMapValues(x => x.toUpperCase).collect()
//Array((t,T), (t,H), (t,I), (t,S), (i,I), (i,S), (a,A), (s,S), (s,P), (s,A), (s,R), (s,K), (c,C), (c,O), (c,L), (c,L), (c,E), (c,C), (c,T), (c,I), (c,O), (c,N)
//extract keys , values
rdd2.keys.collect() // Array(t, i, a, s, c)
rdd2.values.collect() //Array(This, is, a, spark, collection)
////lookup by key
rdd2.lookup("s") //  WrappedArray(spark)
//aggregation
rdd1.flatMap(x => x.toLowerCase.toSeq).map(x => (x , 1)).collect()
//Array((t,1), (h,1), (i,1), (s,1), (i,1), (s,1), (a,1), (s,1), (p,1), (a,1), (r,1), (k,1), (c,1), (o,1), (l,1), (l,1), (e,1), (c,1), (t,1), (i,1), (o,1), (n,1))
///countByKey
rdd1.flatMap(x => x.toLowerCase.toSeq).map(x => (x , 1)).countByKey()
//Map(e -> 1, s -> 3, n -> 1, t -> 2, a -> 2, i -> 3, l -> 2, p -> 1, c -> 2, h -> 1, r -> 1, k -> 1, o -> 2)
//reduce by
def addFunc(left:Int, right:Int) = left + right
rdd1.flatMap(x => x.toLowerCase.toSeq).map(x => (x , 1)).reduceByKey(addFunc).collect()
// Array((p,1), (t,2), (h,1), (n,1), (r,1), (l,2), (s,3), (e,1), (a,2), (i,3), (k,1), (o,2), (c,2))
rdd1.flatMap(x => x.toLowerCase.toSeq).map(x => (x , 1)).groupByKey().map(x => (x._1 ,x._2.reduce(addFunc)))collect()

