import sys
from pyspark.sql.functions import sum as sum_col
import math
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

def main(user_id,table_name):
    df=spark.read.format("org.apache.spark.sql.cassandra").options( table = table_name, keyspace = user_id).load().cache()
    df.show()
    calc_df = df.groupBy(df.host).agg(functions.count(df.path).alias('count_req'),functions.sum(df.bytes).alias('sum_bytes'))
    calc_df=calc_df.withColumn('x_2',pow(calc_df.count_req,2))
    calc_df=calc_df.withColumn('y_2',pow(calc_df.sum_bytes,2))
    calc_df=calc_df.withColumn('x_y',(calc_df.count_req*calc_df.sum_bytes))
    #calc_df.show()
    total_n=calc_df.count()
    sum_val=calc_df.agg(sum_col(calc_df.count_req),sum_col(calc_df.sum_bytes),sum_col(calc_df.x_2),sum_col(calc_df.y_2),sum_col(calc_df.x_y)).collect()
    sum_x_i=sum_val[0][0]
    sum_y_i=sum_val[0][1]
    sum_x_i_2=sum_val[0][2]
    sum_y_i_2=sum_val[0][3]
    sum_x_y=sum_val[0][4]
    num=(total_n*sum_x_y)-(sum_x_i *sum_y_i)
    x2=sum_x_i**2
    y2=sum_y_i**2
    den_1=math.sqrt((total_n*sum_x_i_2)-x2)
    den_2=math.sqrt((total_n*sum_y_i_2)-y2)
    denom=den_1*den_2
    r=(num/denom)
    r_2=r**2

    r_c=calc_df.corr('count_req','sum_bytes')
    print("\n\n\nvalue of calculated r:")
    print(r)
    #print("\n\ncorrelation function used value:")
    #print(r_c)
    print("\n\nvalue of r^2")
    print(r_2)
    print("\n\n")

if __name__ == '__main__':
        user_id = sys.argv[1]
        table_name = sys.argv[2]
        main(user_id,table_name)

