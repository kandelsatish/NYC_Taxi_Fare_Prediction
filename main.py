import pandas as pd
import numpy as np
from utils import variables,functions
import os



if __name__ == "__main__":
    # create spark session
    spark = functions.create_spark_session()

    # load train.csv file
    df=functions.load_data(spark,os.path.join(variables.raw_data_path,"train.csv"),"csv")
    df.show(5)
   
    # stop the spark session when done
    spark.stop()
    print('Spark session stopped')