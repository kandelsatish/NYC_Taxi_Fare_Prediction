import variables
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import zipfile
import mysql.connector
from mysql.connector import errorcode


# function to create spark session
def create_spark_session():
    """Create a Spark Session """
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName(variables.app_name)
        .master("local[*]")
        .config("spark.pyspark.python", variables.python_path) \
        .config("spark.pyspark.driver.python", variables.python_path) \
        .getOrCreate()
    )


# function to extract the zip file
def unzip_file(zip_path, extract_path):
    '''
    params:
    zip_path: path where the zip file is located
    extract_path: path where files is to be extracted

    '''
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
        print("File unzipped successfully")

# unzip_file(zip_path, extract_path)


# function to read csv file
def read_csv(spark,file_path):
    '''
    params:
    session: session object
    file_path: location where the file is located
    n_rows: number of rows to be read

    '''
    df = spark.read.csv(file_path, header=True, inferSchema=True).limit(variables.max_rows)
    return df

#function to read json file
def read_json(spark, file_path):
    '''
    Parameters:
    spark : SparkSession
        Spark session object.
    file_path : str
        Location where the file is located.
    n_rows : int
        Number of rows to be read from the file.
    
    Returns:
    DataFrame
        Spark DataFrame containing the file data.
    '''
    df = spark.read.json(file_path).limit(variables.max_rows)
    return df

# funtion to read parquet file
def read_parquet(spark, file_path):
    '''
    Parameters:
    spark : SparkSession
        Spark session object.
    file_path : str
        Location where the file is located.
    n_rows : int
        Number of rows to be read from the file.
    
    Returns:
    DataFrame
        Spark DataFrame containing the file data.
    '''
    df = spark.read.parquet(file_path).limit(variables.max_rows)
    return df



# function to check if the partition in required and calculate the number of partition required
def partition_needed(spark,df):
    '''
    Parameters:
    spark : SparkSession
        Spark session object.
    df : DataFrame
        Number of rows to be read from the file.
    
    Returns:none
    '''
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





# function to connect with mysql server
def get_connection(user, password, host, database,port=3306):
    '''
    Parameters: 
    user, password, host, port, database
    '''
    try:
        cnx = mysql.connector.connect(host=host,
                                      user=user,
                                      password=password,
                                      port=port,
                                      database=database)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()


get_connection( host=variables.mysql_host,
user=variables.mysql_user,
password=variables.mysql_password,
port=int(variables.mysql_port),
database=variables.mysql_database
)



