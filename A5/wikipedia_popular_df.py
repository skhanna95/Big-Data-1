'''                 BIG DATA ASSIGNMENt-5
                    CMPT-732
                    SHRAY KHANNA
                    301367221
'''
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wikipedia').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    new_path=path.split('/')
    last_elt=new_path[-1]
    filename=last_elt[11:22]
    return filename
comments_schema = types.StructType([ # commented-out fields won't be read
    types.StructField('project_name', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('requests', types.IntegerType(), True),
    types.StructField('bytes', types.LongType(), True)
])
def main(inputs, output):
    wiki_input = spark.read.csv(inputs, schema=comments_schema,sep=' ').withColumn('filename',path_to_hour(functions.input_file_name()))
    wiki_input=wiki_input.filter(wiki_input['project_name'].startswith('en') &~ wiki_input['title'].startswith('Main_Page') &~ wiki_input['title'].startswith('Special:'))
    max_df=wiki_input.groupby(wiki_input['filename']).agg(functions.max(wiki_input['requests']).alias('max')).cache()
    
    max_df=max_df.withColumnRenamed('filename','hour_req')
    final_t=max_df.join(wiki_input,(max_df.max==wiki_input.requests)&(max_df.hour_req==wiki_input.filename))
    #final_t.orderBy(final_t.hour_req).show()
    final_t=final_t.withColumnRenamed('hour_req','hour')
    final_t=final_t.withColumnRenamed('max','views')
    new_df=final_t.select(final_t.hour,final_t.title,final_t.views)
    new_df=new_df.orderBy(new_df.hour)
    new_df.coalesce(1).write.json(output,mode='overwrite')
    #final_t.orderBy(final_t.filename).show()
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)