from kafka import KafkaProducer
from pyspark.sql import SparkSession, functions
import sys
from pyspark.sql.functions import sum as sum_col
spark = SparkSession.builder.appName('Read_Stream').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
topic = sys.argv[1]
messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
values = messages.select(messages['value'].cast('string'))
split_col = functions.split(values.value, ' ')
values.printSchema()
values = values.withColumn('x', split_col.getItem(0).cast('float'))
values = values.withColumn('y', split_col.getItem(1).cast('float'))
values = values.withColumn('xy',values.x*values.y)
values = values.withColumn('xsq',values.x*values.x)

calc_df=values.agg(functions.sum('x').alias('sumx'),functions.sum('y').alias('sumy'),\
	functions.sum('xy').alias('sumxy'),functions.sum('xsq').alias('sumxsq'),functions.count('x').alias('countdf'))
slope_df=calc_df.select(
	((calc_df.sumxy-(1/calc_df.countdf*(calc_df.sumx*calc_df.sumy)))/(calc_df.sumxsq\
		-(1/calc_df.countdf*(calc_df.sumx*calc_df.sumx)))).alias('slope'),
	calc_df.sumx,
	calc_df.sumy,
	calc_df.countdf
	)
intercept_df= slope_df.select(
	slope_df.slope,
	((slope_df.sumy- (slope_df.slope * slope_df.sumx))/ slope_df.countdf).alias('intercept')
	)
stream = intercept_df.writeStream.format('console').outputMode('update').start()
stream.awaitTermination(300)