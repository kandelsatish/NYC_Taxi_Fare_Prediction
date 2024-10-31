import pandas as pd
import numpy as np
from utils import variables,functions
import os



if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] =variables.python_path
    # create spark session
    spark = functions.create_spark_session()

    # read csv file
    df=functions.read_csv(spark,os.path.join(variables.raw_data_path, "train.csv"))
    functions.partition_needed(spark,df)
   
    # stop the spark session when done
    spark.stop()
    print('Spark session stopped')