import time

from flask import Flask, jsonify
from azure.storage.blob import BlockBlobService
from azure_customlib.process_pipeline import Pipeline

# CONFIG
CONFIG = {
        "storage_name": "storagealternance",
        "account_key": "ziygUeiiiGZElpbMZEYBo3r5YjUFuryyvSB8wq3UMpsJWl9hWdjDxnvWBin+EHcuhB+KEd2G3C33dGX+YifCMg==",
        "container": "clementeve",

        "cluster_name": "sshuser",
        "cluster_key": "Supermotdepasse!42",

        "data_dir": "datas/",
        "temp_result_dir": "temp_results/",
        "result_dir": "results/",

        "processed_list_path": "processed/processed_list.txt",
        "error_list_path": "errors/errors_list.txt",
        "log_path": "logs/activity.log"
    }


# INIT
app = Flask(__name__)
block_blob_service = BlockBlobService(account_name=CONFIG['storage_name'], account_key=CONFIG['account_key'])

app.processing = False


@app.route("/start_pipeline", methods=['POST'])
def post():
    print("Start pipeline")
    count = 0
    while True:

        pipeline = Pipeline(CONFIG)
        files_nb = pipeline.run()

        if count == 10:
            break
            
        if files_nb == 0:
            print("no file to process")
            count += 1
            print("retry n°" + str(count))
            time.sleep(30)

        time.sleep(60)

    return "Processed"


@app.route("/get_logs", methods=['GET'])
def get():
    # Download log
    blob = block_blob_service.get_blob_to_text(container_name=CONFIG['container'], blob_name=CONFIG['log_path'])
    logs = blob.content

    return logs


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9000', debug=True)
