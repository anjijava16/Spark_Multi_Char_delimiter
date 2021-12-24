import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
rawData = sc.textFile("C:/data/sparkspecialdata/welcome.csv")
rowRDD = rawData.map(lambda  res: res.split('||'))
fields="country,age,salary,purchased,ts"
fields=[StructField(fs_name,StringType(),True) for fs_name in fields.split(',')]
schema=StructType(fields)
df=spark.createDataFrame(rowRDD,schema)
