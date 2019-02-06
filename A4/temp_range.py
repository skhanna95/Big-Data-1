import sys
from pyspark.sql import SparkSession, functions, types
#from pyspark import SparkConf, SparkContext

spark = SparkSession.builder.appName('weather ETL').getOrCreate()


observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])


def main(in_directory, out_directory):
    inp_data = spark.read.csv(in_directory, schema=observation_schema)
    # TODO: finish here.
    weather1 = inp_data.filter((inp_data['qflag'].isNull()) & (inp_data['observation']=='TMAX') | (inp_data['observation']=='TMIN')).cache()
    weather1.show()
    #weather_TMAX = inp_data.filter(weather.observation=='TMAX').cache()
    #weather_TMAX.show()
    #weather = weather.filter((weather['qflag'].isNull()) & ((weather['observation']=='TMAX')|(weather['observation']=='TMIN')))
    '''cleaned_data_weather = weather.select(
        weather['station'],
        weather['date'],
        (weather['value']/10).alias('tmax'),
    )

    #cleaned_data_weather.show()
    cleaned_data_weather.write.json(out_directory, compression='gzip', mode='overwrite')
    '''

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)