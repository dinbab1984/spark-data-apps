// Execution --> Job --> Stages (sometimes skipped) --> Tasks (partition level + shuffles , etc.,)
val ds1 = spark.range(2,1000000,2) //e.g. Stage 1 : 4 - 8 partitions (tasks)
val ds2 = spark.range(2,1000000,4) //e.g. Stage 2 : 4 - 8 partitions (tasks)
val step1 = ds1.repartition(5) //e.g. Stage 3 : repartition  / shuffles : 5 tasks
val step12 = ds2.repartition(6) // e.g. Stage 4 : repartition / shuffles : 6 tasks
val step2 = step1.selectExpr("id * 5 as id") // e.g. on Stage 1 itself
val step3 = step2.join(step12, "id") // e.g. Stage 5 : join / shuffles (default : spark.sql.shuffle.partitions = 200 tasks)
val step4 = step3.selectExpr("sum(id)") // e.g. Stage 6 : 1 task (aggregation)
step4.collect()

