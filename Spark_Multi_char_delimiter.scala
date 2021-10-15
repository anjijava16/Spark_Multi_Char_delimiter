import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


val customSchema = StructType(Array(
  StructField("country", StringType, true),
  StructField("age", StringType, true),
  StructField("salary", StringType, true),
  StructField("purchased", StringType, true),
  StructField("ts", StringType, true))
)
 val rawData = sc.textFile("C:/data/sparkspecialdata/welcome.csv")
 val rowRDD = rawData.map(line => Row.fromSeq(line.split("\\|\\|")))
 
 val df = spark.createDataFrame(rowRDD, customSchema)
 df.printSchema()
 scala> df.printSchema()
root
 |-- country: string (nullable = true)
 |-- age: string (nullable = true)
 |-- salary: string (nullable = true)
 |-- purchased: string (nullable = true)
 |-- ts: string (nullable = true)


scala> df.show(10)
+-------+---+------+---------+-------------------+
|country|age|salary|purchased|                 ts|
+-------+---+------+---------+-------------------+
| France| 44|  7200|       NO|2021-07-02 17:06:19|
|  Spain| 27|  4800|      Yes|2020-07-02 17:06:19|
|Germany| 30|  5400|       NO|2019-07-02 17:06:19|
| France| 38|  6100|       NO|2021-06-02 17:06:19|
|  Spain| 40|  7200|       NO|2021-07-04 17:06:19|
|Germany| 35|  7200|       NO|2018-07-02 17:06:19|
| France|   |  7200|       NO|2017-07-02 17:06:19|
|  Spain| 48|  7200|       NO|2016-07-02 17:06:19|
|Germany| 50|  7200|      Yes|2015-07-02 17:06:19|
+-------+---+------+---------+-------------------+

