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
	# Create Orders view
	orders_df = spark.read.format("org.apache.spark.sql.cassandra").options( table='orders',keyspace=keyspace).load()
	orders_df.createOrReplaceTempView('orders')

	# Create Parts view
	part_df = spark.read.format("org.apache.spark.sql.cassandra").options( table = 'part', keyspace=keyspace).load()
	part_df.createOrReplaceTempView('part')

	# Create LineItems view
	line_item_df = spark.read.format("org.apache.spark.sql.cassandra").options( table='lineitem', keyspace=keyspace).load()
	line_item_df.createOrReplaceTempView('lineitem')

	# Join the tables with SQL query
	join_table = spark.sql('''
	                       select o.orderkey,o.totalprice, p.name from Orders o 
	                       join lineitem l on o.orderkey = l.orderkey
	                       join part p ON l.partkey = p.partkey
	                       where o.orderkey in
	                       ''' + str(orderkeys))

	# Make parts from same orders into single row and parts as comma separated
	formatted_summary = join_table.groupBy('orderkey', 'totalprice').\
	    agg(functions.collect_set('name')).alias('names')
	formatted_summary=formatted_summary.orderBy(formatted_summary.orderkey)
	formatted_summary.show()
	lines = formatted_summary.rdd
	lines=lines.map(output_line)
	lines.coalesce(1).saveAsTextFile(outdir)

if __name__ == '__main__':
	keyspace = sys.argv[1]
	outdir = sys.argv[2]
	orderkeys = tuple(sys.argv[3:])
	main(keyspace,outdir,orderkeys)