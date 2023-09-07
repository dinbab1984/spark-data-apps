//creating rdd (resilient distributed dataset)
spark.range(3).rdd.collect()
//to Row Object
spark.range(3).rdd.toDF().collect(
//access rdd object
spark.range(3).rdd.toDF().map(x => x.getLong(0)).collect()
//from collection
val col1 = Array(2,4,6)
val rdd1 = spark.sparkContext.parallelize(col1)
val col2 = "This is collection"
val rdd2 = spark.sparkContext.parallelize(col2.split(" "))
rdd2.setName("collection") // name shows on UI
rdd2.name
//rdd fron fils
val rdd3 = spark.sparkContext.textFile("data/flight-data/csv/2010-summary.csv")//each line is each line in file
val rdd4 = spark.sparkContext.wholeTextFiles("data/flight-data/csv/*")//each line is each file, two objs - filename, data
//Manipulating rdds
rdd2.count()
//distinct
rdd2.distinct().count()
//filter e.g. text length > 3
def textLongerThan3(str: String):Boolean = {
    if(str.length > 3){
    return true
    }
    return false
    }
rdd2.filter(x => textLongerThan3(x)).collect()
//map
rdd2.map(x => (x , x(1),textLongerThan3(x))).collect()
//flatMap
rdd2.flatMap(x => x.toSeq).collect()
//sortBy
rdd2.sortBy(x => x.length).collect()
rdd2.sortBy(x => x.length * -1).collect() //desc
//randomSplit
val rdd2_1 = rdd2.randomSplit(Array(0.5, 0.5))
rdd2_1(0).collect()//Array(is, collection)
rdd2_1(1).collect()//Array(This)

//Actions
//reduce
spark.sparkContext.parallelize(1 to 10).reduce(_+_) // 55
def longWord(left:String, right:String):String = {
    if(left.length > right.length){
        return left
    }
    return right
}
rdd2.reduce(longWord)
//count, first, main, max, distinct, take, takeOrdered, top
rdd2.count()
rdd2.countByValue()
rdd2.first()//This
rdd3.min()//This
rdd2.max() //is
rdd2.distinct()
rdd2.take(3)
rdd2.takeOrdered(2)
rdd2.top(3)
//save as text file
rdd2.saveAsTextFile("tmp/rdd2_1.txt")
rdd2.saveAsObjectFile("tmp/rdd2_seq")
//cache rdd and getStorageLevel
rdd2.cache()
rdd2.getStorageLevel
//checkpint , presist rdds in disk , so that every time rdd is referenced it comes from disk instead of computing
spark.sparkContext.setCheckpointDir("/tmp/checkpointDir")
rdd2.checkpoint()
//Pipe RDDS to system commands
rdd2.pipe("wc -l").collect()

