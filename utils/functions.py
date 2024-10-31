from utils import variables
from pyspark.sql import SparkSession
from dotenv import load_dotenv



def create_spark_session():
    """Create a Spark Session """
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName(variables.app_name)
        .master("local[*]")
        .getOrCreate()
    )
spark = create_spark_session()
print('Session Started')



def read_csv(spark,file_path):
    '''
    params:
    session: session object
    file_path: location where the file is located
    n_rows: number of rows to be read

    '''
    df = spark.read.csv(file_path, header=True, inferSchema=True).limit(variables.max_rows)
    return df


def partition_needed(spark,df):
    # get the size of the DataFrame
    num_rows = df.count()  # total number of rows
    data_size_mb = df.rdd.map(lambda row: len(str(row))).reduce(lambda a, b: a + b) / (1024 * 1024)  #size in mb

    print("Checking is partition is required")
    # determine if partitioning is needed
    if data_size_mb > 1024:  # If size is greater than 1 GB
        num_partitions = max(2, min(64, num_rows // 100000)) 
        df = df.repartition(num_partitions)
        print("Partitioning is required")
        print(f"DataFrame repartitioned to {num_partitions} partitions.")
    else:
        print("No need to repartition.")

