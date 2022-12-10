from operator import add

from pyspark import SparkConf, SparkContext

config = SparkConf().setAppName("ExampleApp").setMaster("local[4]")
spark_ctx = SparkContext(conf=config)

data = list(range(10))
distributed_data = spark_ctx.parallelize(data)

print(distributed_data)
print(type(distributed_data))

print(distributed_data.reduce(add))
