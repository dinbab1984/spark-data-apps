//create simple data frames
val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100))
    ).toDF("id", "name", "graduate_program", "spark_status")
val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")
    ).toDF("id", "degree", "department", "school")
val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")
    ).toDF("id", "status")

//inner join (default)
person.join(graduateProgram, person.col("graduate_program") === graduateProgram.col("id")).show(false)
person.join(graduateProgram, person.col("graduate_program") === graduateProgram.col("id"),"inner").show(false)

//outer join
 person.join(graduateProgram, person.col("graduate_program") === graduateProgram.col("id"),"outer").show(false)

 //left outer join
 person.join(graduateProgram, person.col("graduate_program") === graduateProgram.col("id"),"left_outer").show(false)

 //right outer join
 person.join(graduateProgram, person.col("graduate_program") === graduateProgram.col("id"),"right_outer").show(false)

 //left semi
 graduateProgram.join(person, person.col("graduate_program") === graduateProgram.col("id"),"left_semi").show(false)

 //left anti
 graduateProgram.join(person, person.col("graduate_program") === graduateProgram.col("id"),"left_anti").show(false)

 //cross join
 graduateProgram.crossJoin(person).show(false)

 //complex join
 person.join(sparkStatus,array_contains(person.col("spark_status"),sparkStatus.col("id"))).show()

 //handling duplicate column names
 //df.column name
 person.join(sparkStatus,array_contains(person.col("spark_status"),sparkStatus.col("id"))).select(person.col("id")).show()
 //rename join columns with same name and specific just column name instead of join experssion
graduateProgram.withColumnRenamed("id","graduate_program").join(person,"graduate_program").select("graduate_program").show(false)
//drop  column after join
graduateProgram.join(person,person.col("graduate_program") === graduateProgram.col("id")
    ).drop(graduateProgram.col("id")).select("graduate_program").show()
//rename before join
val renamed_graduateProgram = graduateProgram.withColumnRenamed("id","grad_id")
renamed_graduateProgram.join(person,person.col("graduate_program") === renamed_graduateProgram.col("grad_id")).show()

//how spark perform joins

