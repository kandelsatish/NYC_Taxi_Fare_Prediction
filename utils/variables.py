"""
Global variables
"""



import json
import os

global app_name

global mysql_host
global mysql_user
global mysql_password
global mysql_database
global mysql_port

global mongodb_host
global mongodb_user
global mongodb_password
global mongodb_database
global mongodb_port

global raw_db
global processed_db

global raw_data_path
global processed_data_path
global max_rows
global python_path


# get the path of the config file
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_file_path = os.path.join(root_dir, "config", "config.json")

raw_data_path=os.path.join(root_dir,"data","raw_data")
processed_data_path=os.path.join(root_dir,"data","processed_data")
print(raw_data_path,processed_data_path)

# load config file
def load_config(config_file_path):
    with open(config_file_path, 'r') as file:
        config = json.load(file)
    return config


config_file=load_config(config_file_path)

app_name=config_file['__name__']

mysql_host=config_file["connection"]['mysql']['host']
mysql_user=config_file["connection"]['mysql']['user']
mysql_password=config_file["connection"]['mysql']['password']
mysql_database=config_file["connection"]['mysql']['database']
mysql_port=config_file["connection"]['mysql']['port']

mongodb_host=config_file["connection"]['mysql']['host']
mongodb_user=config_file["connection"]['mysql']['user']
mongodb_password=config_file["connection"]['mysql']['password']
mongodb_database=config_file["connection"]['mysql']['database']
mongodb_port=config_file["connection"]['mysql']['port']

raw_db=config_file["connection"]['mysql']['host']
processed_db=config_file["connection"]['mysql']['host']
max_rows=config_file['max_rows']
python_path=config_file['python_path']


