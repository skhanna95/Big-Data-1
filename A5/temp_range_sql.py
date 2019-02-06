'''                 BIG DATA ASSIGNMENt-5
                    CMPT-732
                    SHRAY KHANNA
                    301367221
'''

import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession \
    .builder \
    .appName("temp range sql") \
    .getOrCreate()

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]

    weather_schema = StructType([
        StructField('station', StringType(), False),
        StructField('date', StringType(), False),
        StructField('observation', StringType(), False),
        StructField('value', IntegerType(), False),
        StructField('MFLAG', StringType(), True),
        StructField('QFLAG', StringType(), True),
        StructField('SFLAG', StringType(), True),
        StructField('SFLAG2', StringType(), True),
    ])

    df = spark.read.csv(path = inputs, header = True, schema = weather_schema)
    df = df.where(df['QFLAG'].isNull()).cache()

    df.createOrReplaceTempView('df')

    dfMax = spark.sql("""
        SELECT station, date, 1.0*(value/10) as max
        FROM df WHERE observation == 'TMAX'
        """).createOrReplaceTempView('dfMax')

    dfMin = spark.sql("""
        SELECT station, date, 1.0*(value/10) as min
        FROM df WHERE observation == 'TMIN'
        """).createOrReplaceTempView('dfMin')

    del df
    spark.catalog.dropTempView("df")

    joinedDF = spark.sql("""
    SELECT t1.station, t1.date, (t1.max - t2.min) as range
    FROM dfMax t1
    JOIN dfMin t2
    ON t1.station = t2.station AND t1.date = t2.date
    """).cache().createOrReplaceTempView('joinedDF')

    maxRangeDF = spark.sql("""
    SELECT date, MAX(range) AS maxrange FROM joinedDF
    GROUP BY date
    """).createOrReplaceTempView('maxRangeDF')

    result = spark.sql("""
    SELECT t1.station as station, t1.date as date, t2.maxrange as range
    FROM joinedDF t1
    JOIN maxRangeDF t2
    ON t1.date = t2.date AND t1.range = t2.maxrange
    """).cache()
    result=result.orderBy(result.date)
    result.write.csv(output,mode='overwrite')

    spark.catalog.dropTempView("joinedDF")
    spark.catalog.dropTempView("maxRangeDF")
    spark.catalog.dropTempView("dfMin")
    spark.catalog.dropTempView("dfMax")
    #del result

if __name__ == "__main__":
    main()