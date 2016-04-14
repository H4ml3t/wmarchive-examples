# Run with parameter --packages com.databricks:spark-avro_2.10:1.0.0
# E.g. spark-shell --packages com.databricks:spark-avro_2.10:1.0.0

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark fwjr JSONs 2 parquet")
sc = SparkContext(conf=conf)

df = sqlContext.load("/cms/wmarchive/test/avro/2016/01/01/", "com.databricks.spark.avro")

df.printSchema()

df.count()

aggregation1 = df.select("steps.performance.cpu") \
    .rdd \
    .flatMap(lambda cpuArrayRows: cpuArrayRows[0]) \
    .map(lambda row: row.asDict()) \
    .flatMap(lambda rowDict: [(k,v) for k,v in rowDict.iteritems()]) \
    .reduceByKey(lambda x,y: x+y)
    
# Store the file as a simple text file
aggregation1.saveAsTextFile("wmarchive/test-plaintext-aggregation1")

aggregated1DF = sqlContext.createDataFrame([{v[0]:v[1] for v in aggregation1.collect()}])

# saving in Json format
aggregated1DF.toJSON().saveAsTextFile("wmarchive/test-json-aggregation1")

# how to write in Avro format
aggregated1DF.save("wmarchive/test-avro-aggregation1", "com.databricks.spark.avro")