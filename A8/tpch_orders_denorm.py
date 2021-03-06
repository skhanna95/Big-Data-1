from pyspark import SparkConf
from pyspark import SparkConf

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def output_line(line):
	orderkey=line[0]
	price=float(line[1])
	names=line[2]
	namestr = ', '.join(sorted(list(names)))
	return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def main(keyspace,outdir,orderkeys):
	# Create Orders_Parts data frame
	nava_orders_parts_df = spark.read.format("org.apache.spark.sql.cassandra")\
	.options( table = 'orders_parts', keyspace = keyspace).load()

	nava_orders_parts_df=orders_parts_df.select('orderkey','totalprice','part_names').where('orderkey in' + str(orderkeys))
	nava_orders_parts_df=nava_orders_parts_df.orderBy(nava_orders_parts_df.orderkey)
	nava_orders_parts_df.show()
	# Map the data frame as rdd into the required format
	lines = nava_orders_parts_df.rdd
	lines=lines.map(output_line)
	# Save into the output directory
	lines.coalesce(1).saveAsTextFile(outdir)

if __name__ == '__main__':
	keyspace = sys.argv[1]
	outdir = sys.argv[2]
	orderkeys = tuple(sys.argv[3:])
	main(keyspace,outdir,orderkeys)