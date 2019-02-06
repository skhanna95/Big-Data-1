'''                 BIG DATA ASSIGNMENt-5
                    CMPT-732
                    SHRAY KHANNA
                    301367221
'''
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
    weather = spark.read.csv(in_directory, schema=observation_schema)
    # INPUT THE DATA AND FILTER ACCORDING TO THE CONDITIONS
    weather = weather.filter((weather['qflag'].isNull()) & ((weather['observation']=='TMAX')|(weather['observation']=='TMIN')))
    
    #MAX TEMPERATURES FROM THE INPUT
    weather_TMAX = weather.filter(weather.observation=='TMAX')
    weather_TMAX=weather_TMAX.select(
        weather_TMAX['station'],
        weather_TMAX['date'],
        (weather_TMAX['value']/10).alias('tmax'),
    )

    #MIN TEMPERATURES FROM THE INPUT
    weather_TMIN = weather.filter(weather.observation=='TMIN')
    #weather_TMIN.show()
    weather_TMIN=weather_TMIN.select(
        weather_TMIN['station'],
        weather_TMIN['date'],
        (weather_TMIN['value']/10).alias('tmin'),
    )
    
    weather_TMAX=weather_TMAX.withColumnRenamed('date','max_date')
    weather_TMAX=weather_TMAX.withColumnRenamed('station','max_station')
    
    #COMBINING MAX TEMPERATURES AND MIN TEMPERATURES
    weather_join=weather_TMIN.join(
        weather_TMAX,
        (weather_TMIN.date==weather_TMAX.max_date)&(weather_TMIN.station==weather_TMAX.max_station)
        )

    #CALCULATION OF TEMPERATURE RANGE
    weather_diff=weather_join.select(
        weather_join.station,
        weather_join.date,
        (weather_join.tmax - weather_join.tmin).alias('diff')
        )

    #GETTING MAX FROM THE RANGE
    weather_max_range=weather_diff.groupby(weather_diff.date).agg(functions.max(weather_diff.diff).alias('range'))
    weather_max_range=weather_max_range.withColumnRenamed('date','rdate')
    
    #JOIN WITH INITIAL DATASET FOR CHECKING IF THERE'S MORE THAN 1 MAX
    weather_output=weather_max_range.join(
        weather_diff,
        (weather_max_range.rdate == weather_diff.date) &
        (weather_max_range.range == weather_diff.diff)
    ) 
    weather_final=weather_output.select(weather_output.date,weather_output.station,weather_output.range)
    weather_final=weather_final.orderBy(weather_final.date)

    weather_final.coalesce(1).write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)