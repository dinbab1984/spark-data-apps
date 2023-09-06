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

//regular expression
//regex_Replace
val list = Seq("Blue Sky","Green Leaf","Red Soil","White Milk")
val sc_rdd = sc.parallelize(list)
sc_rdd.collect()
sc_rdd.toDF().select(regexp_replace(upper(col("value")),"WHITE|RED|GREEN|BLUE","Some Color").alias("Replaced"),col("value")).show(false)

//replace characters with another characters
sc_rdd.toDF().select(translate(upper(col("value")),"KYM","EAF").alias("Replaced"),col("value")).show(false)

//regexp_extract
sc_rdd.toDF().select(regexp_extract(upper(col("value")),"(WHITE|RED|GREEN|BLUE)",1).alias("extracted"),col("value")).show(false)

//contains text
sc_rdd.toDF().select(col("value"),upper(col("value")).contains("RED").or(upper(col("value")).contains("BLUE")).alias("RedorBlue?")).show()

// adding dynamic columns based on the list of values
c_rdd.toDF().select(color.map(c => { upper(col("value")).contains(c).alias(s"is_$c")}):+expr("*"):_*).show()
//:+expr("..") -- append columns
//:_* (seems syntax for dynamic columns)

//Working with Dates and Timestamps
val df = spark.range(1)
//current date
df.withColumn("current_date",current_date()).show()
//current timestamp
 df.withColumn("current_timestamp",current_timestamp()).show(false)
//add days to current date 
df.withColumn("today+7",date_add(current_date,7)).show()
//subtract days to current date 
df.withColumn("today_time-7",date_sub(current_timestamp,7)).show()
//date difference
df.withColumn("datediff",datediff(current_date(),to_date(lit("2023-12-31")))).show()
df.withColumn("datediff",datediff(current_date(),to_date(lit("2022-12-31")))).show()
//months_between
df.withColumn("months",months_between(current_date(),to_date(lit("2022-12-31")))).show()
// to date + date time format
 df.withColumn("date1",to_date(lit("2022-12-31"))).show()
 df.withColumn("date1",to_date(lit("2022-13-31"))).show() // return null in case wrong date
 df.withColumn("date1",to_date(lit("2022-12-31"),"yyyy-MM-dd")).show()
 df.withColumn("date1",to_timestamp(lit("2022-12-31 23:59:15"),"yyyy-MM-dd HH:mm:ss")).show()
 df.withColumn("date1",to_timestamp(lit("2022-12-31 23:59:75"),"yyyy-MM-dd HH:mm:ss")).show() // return null in case wrong date or timestamp

 //working with null values
df.selectExpr("coalesce(null,'test') as test").show()
df.selectExpr("ifnull(null,'test') as test").show()
df.selectExpr("nvl(null,'test') as test").show()
df.selectExpr("nvl2(null,null,'test') as test").show()
//drop null rows
val df_null = df.selectExpr("null id","null as dummy")
df_null.na.drop("all").show()
//drop rows if any column is null
val df_any_null = df.selectExpr("id","null as dummy")
df_any_null.na.drop().show() // default if any column is null
df_any_null.na.drop("any").show()
df.na.drop("all", Seq("col1", "col"))// only consider certain columns
//na string fill
val na_fill = Seq(("","LastName"),("FirstName",""),("MyFirst","MyLast"))
val df_na_fill = sc.parallelize(na_fill).toDF("col1","col2")
df_na_fill.withColumn("col1",when(col("col1")==="",null).otherwise(col("col1"))
    ).withColumn("col2",when(col("col2")==="",null).otherwise(col("col2"))
    ).na.fill("i am here when you are null").show(false)
// map fill by column
val na_fill_map = Map("col1" -> "i am here when col1 is null","col2" -> "i am here when col2 is null")
df_na_fill.withColumn("col1",when(col("col1")==="",null).otherwise(col("col1"))
    ).withColumn("col2",when(col("col2")==="",null).otherwise(col("col2"))
    ).na.fill(na_fill_map).show(false)
//replace one string with other by map
df_na_fill.withColumn("col1",when(col("col1")==="",null).otherwise(col("col1"))
    ).withColumn("col2",when(col("col2")==="",null).otherwise(col("col2"))
    ).na.fill(na_fill_map
    ).na.replace("col1",replace_map).na.replace("col2",replace_map).show(false)

//Working with Complex Types 
//structs e.g. dataframe within dataframe
val list = Seq(("Name1","Street1","City1","PostCode1"),("Name2","Street2","City2","PostCode2"))
val rdd_list = sc.parallelize(list)
val df_list = rdd_list.toDF("Name","Street","City","PostCode")
df_list.selectExpr("Name","(Street,City,PostCode) as address").show(false)
df_list.selectExpr("Name","struct(Street,City,PostCode) as address").show(false)
df_list.selectExpr("Name","(Street,City,PostCode) as Address").select("Name","Address.*").show(false)
df_list.selectExpr("Name","(Street,City,PostCode) as Address").select("Name","Address.City").show(false)
df_list.selectExpr("Name","(Street,City,PostCode) as Address").select(col("Name"),col("Address").getField("City")).show(false)

//split, array, array_contains, array_length, explode, maps
val names = Seq(("batch1","john clare anna chris maddy jenny"),("batch2","david william joe tina"))
val df_names = sc.parallelize(names).toDF("Batch","Persons")
df_names.select(col("Batch"),split(col("Persons")," ").alias("Person_list")).show(false)
df_names.select(col("Batch"),split(col("Persons")," ").alias("Person_list")).selectExpr("Person_list[2]").show(false)
df_names.select(col("Batch"),size(split(col("Persons")," ")).alias("Person_count")).show(false)
df_names.select(col("Batch"),array_contains(split(col("Persons")," "),"david").alias("david_found?")).show(false)
df_names.select(col("Batch"),explode(split(col("Persons")," ")).alias("Person")).show(false)
df_names.select(col("Batch"),col("Persons"),explode(split(col("Persons")," ")).alias("Person")).show(false)

//maps
val maps = Seq(("1","IN","India"),("2","DE","Germany"))
val df_maps = sc.parallelize(maps).toDF("no","country_code","country_name")
df_maps.select(map(col("country_code"),col("country_name")).as("country_map")).selectExpr("*","country_map['IN'] as IN_name").show()
df_maps.select(map(col("country_code"),col("country_name")).as("country_map")).select(explode(col("country_map"))).show()

//working with JSON
val df_json = spark.range(1).selectExpr("""'{"data": { "id" : 1, "name": "spark"}}' as jsonString""")
df_json.select(get_json_object(col("jsonString"),"$.data.name").alias("name")).show()
df_json.select(json_tuple(col("jsonString"),"data").alias("data")).show(false)

val df = sc.parallelize(Seq(("IN","India"),("DE","Germany"))).toDF("id","country")
df.select(to_json(struct(col("id"),col("country"))).as("json")).show(false)

import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType}

val schema = StructType(Array(StructField("id",StringType,true),StructField("country",StringType,true)))
df.select(to_json(struct(col("id"),col("country"))).as("json")).select(col("json"),from_json(col("json"),schema).as("parsed")).show(false)


//User Defined functions
def power3(n:Double):Double = n * n * n
power3(3.0)
//register in sparksession
val udf_power3 = udf(power3(_:Double):Double)
df.select(udf_power3(col("id"))).show()
//register as sql function
spark.udf.register("power3",power3(_:Double):Double)
df.selectExpr("power3(id)").show() // use with expression


