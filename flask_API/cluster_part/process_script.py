import json
import argparse
from pyspark.sql import SparkSession

config_file = "/home/sshuser/eve_clement/config.json"

# CONFIG
# 1. Parse script arguments
parser = argparse.ArgumentParser()
parser.add_argument("files", help="List of Azure path of files to process separated by ':' ")
args = parser.parse_args()

files_list = args.files.split(':')

# 2. Parse config file
with open(config_file) as json_file:
    config = json.loads(json_file.read())

storage_name = config['storage_name']
account_key  = config['account_key']
container = config['container']
result_dir = config['result_dir']


# LOAD SPARK
spark = SparkSession.builder.appName("ProjetBigData").getOrCreate()
spark.conf.set(
    "fs.azure.account.key.{}.blob.core.windows.net".format(storage_name), account_key
)


# PROCESSING
# 1. Read file and creates temp view
files_path = ["wasbs://{}@{}.blob.core.windows.net/{}".format(container, storage_name, file) for file in files_list]
df = spark.read.format('csv').options(header='true', inferSchema='true').load(files_path)
df.createOrReplaceTempView("transactions")


# 2. Function used to request and save result of request
def request_and_save(request, request_name):
    result_path = result_dir.format(request_name)
    # Request
    result = spark.sql(request)
    result.createOrReplaceTempView("result")
    # Open result file on azure
    try:
        previous_result = spark.read.format('csv').options(header='true', inferSchema='true').load(result_path + "/*")
        previous_result.createOrReplaceTempView("old_result")
        # Compute new results
        old_mean = spark.sql("select mean_amount from old_result")
        old_count = spark.sql("select count_amount from old_result")
        new_mean = spark.sql("select mean_amount from result")
        new_count = spark.sql("select count_amount from result")

        # TODO : REPLACE THIS WITH NEW VALUE
        result_df = result

    except:
        result_df = result

    # TODO : CHANGE FILENAME
    # Save result to azure
    result_df.coalesce(1)\
        .write\
        .format('csv')\
        .save("wasbs://{}@{}.blob.core.windows.net/{}".format(container, storage_name, result_path), header='true')

    return


# 3. Use function to compute a request (Amount of send by company) and save result
request_country_send = """select account_sender_name, mean(amount) as mean_amount, 
                        std(amount) as std_amount, 
                        sum(amount) as sum_amount, 
                        count(amount) as count_amount 
                        from transactions 
                        GROUP BY account_sender_name"""

request_and_save(request_country_send, "result")
