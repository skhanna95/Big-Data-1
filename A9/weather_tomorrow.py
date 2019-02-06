import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3
import datetime
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.types import DateType as dt
tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(out_model):
    #new_d=datetime.date(2018-11-12)
    new_d=datetime.date(2018, 11, 12)
    new_d2=datetime.date(2018, 11, 13)
    tom_df=spark.createDataFrame([('RSM00031235', new_d,49.2771,-122.9146,330.0,12.0),\
        ('RSM00031235', new_d2,49.2771,-122.9146,330.0,13.0)],\
        tmax_schema)
    
    tom_df.show()
    model = PipelineModel.load(out_model)

    predictions = model.transform(tom_df)
    predictions.show()
    
    result=predictions.select("prediction").collect()
    prediction=result[0]['prediction']
    print('Predicted tmax tomorrow:',prediction)

if __name__ == '__main__':
    out_model=sys.argv[1]
    main(out_model)

