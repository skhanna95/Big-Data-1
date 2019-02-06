import sys
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
from pyspark import SparkConf
sc = spark.sparkContext
import datetime
import uuid

def parseline(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = re.search(line_re, line)
    if match:
        m = re.match(line_re, line)
        host = m.group(1)
        path=m.group(3)
        date=datetime.datetime.strptime(m.group(2), '%d/%b/%Y:%H:%M:%S')
        bys = int(m.group(4))
        iden= str(uuid.uuid1())
        return host,iden,date, path, bys
    return None

http_schema = types.StructType([
    types.StructField('host', types.StringType(), False),
    types.StructField('id',types.StringType(),False),
    types.StructField('datetime', types.TimestampType(), False),
    types.StructField('path', types.StringType(), False),
    types.StructField('bytes', types.IntegerType(), False),
])

def main(input_dir,user_id,table_name):
	text = sc.textFile(input_dir).cache()
	text = text.map(parseline).filter(lambda x: x is not None)
	text = text.repartition(100)
	df=spark.createDataFrame(text,http_schema)
	df.write.format("org.apache.spark.sql.cassandra").options( table = table_name, keyspace = user_id).save(mode="append")

if __name__ == '__main__':
	input_dir = sys.argv[1]
	user_id = sys.argv[2]
	table_name = sys.argv[3]
	main(input_dir,user_id,table_name)