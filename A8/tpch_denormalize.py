from pyspark import SparkConf
from pyspark import SparkConf

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def main(input_key_space,output_key_space):
	'''
	CREATE TABLE orders_parts (
	  orderkey int,
	  custkey int,
	  orderstatus text,
	  totalprice decimal,
	  orderdate date,
	  order_priority text,
	  clerk text,
	  ship_priority int,
	  comment text,
	  part_names set<text>,
	  PRIMARY KEY (orderkey)
	);
	'''
	# Create Orders view
	orders_df = spark.read.format("org.apache.spark.sql.cassandra").options( table='orders',keyspace=input_key_space).load()
	orders_df.createOrReplaceTempView('orders')

	# Create Parts view
	part_df = spark.read.format("org.apache.spark.sql.cassandra").options( table = 'part', keyspace=input_key_space).load()
	part_df.createOrReplaceTempView('part')

	# Create LineItems view
	line_item_df = spark.read.format("org.apache.spark.sql.cassandra").options( table='lineitem', keyspace=input_key_space).load()
	line_item_df.createOrReplaceTempView('lineitem')

	# Join the tables with SQL query
	join_table = spark.sql('''
	                       select o.*, p.name from Orders o
	                       join lineitem l on o.orderkey = l.orderkey
	                       join part p ON l.partkey = p.partkey
	                       ''')
	formatted_summary = join_table.groupBy('orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate',
	                                       'order_priority', 'clerk', 'ship_priority', 'comment').\
	    agg(functions.collect_set('name').alias('part_names'))
	formatted_summary.show()

	# Convert the data frame to rdd and store it into the de-normalized table
	formatted_summary.write.format("org.apache.spark.sql.cassandra").options( table='orders_parts',keyspace = output_key_space).save(mode="append")

if __name__ == '__main__':
	input_key_space = sys.argv[1]
	output_key_space = sys.argv[2]
	main(input_key_space,output_key_space)