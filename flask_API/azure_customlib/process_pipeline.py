import os
import datetime

from azure.storage.blob import BlockBlobService


class Pipeline(object):
    """
    """
    def __init__(self, config):
        self._storage_name = config['storage_name']
        self._account_key = config['account_key']
        self._container = config['container']

        self._data_dir = config["data_dir"]
        self._result_dir = config["result_dir"]

        self._processed_list_path = config["processed_list_path"]
        self._error_list_path = config["error_list_path"]
        self._log_path = config['log_path']

        self._block_blob_service = self.__azure_init()

        self._logfile, self._log_format = self.__create_log_file()
        self.processed_list = self.__data_list_init(self._processed_list_path)
        self.error_list = self.__data_list_init(self._error_list_path)


    def __azure_init(self):
        return BlockBlobService(account_name=self._storage_name, account_key=self._account_key)


    def __create_log_file(self):
        # Get or create log file
        if self._block_blob_service.exists(self._container, self._log_path):
            b = self._block_blob_service.get_blob_to_text(self._container, self._log_path)
            logfile = b.content
        else:
            logfile = ""

        #set format for log message
        log_format = "%(date)s (utc) :: %(filename)s :: %(message)s\n"

        return logfile, log_format


    def __data_list_init(self, path):
        # Get or create list
        if self._block_blob_service.exists(self._container, path):
            b = self._block_blob_service.get_blob_to_text(self._container, path)
            data_list = b.content.split(";")
        else:
            data_list = []

        return data_list


    def _write_log(self, file, message):
        # Add log message to logfile
        log_infos = {
            "date": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "filename": file,
            "message": message
        }
        log = self._log_format % log_infos
        self._logfile += log

        return


    def clean_container(self):
        # Remove useless files from container and rename result file
        files = self._block_blob_service.list_blob_names(container_name=self._container, prefix=self._result_dir)
        files = [f for f in files if not f == self._result_dir+"result.csv"]

        for f in files:
            if "/part-" not in f:
                # Delete old files
                self._block_blob_service.delete_blob(container_name=self._container, blob_name=f)


    def _process_on_cluster_and_save(self, files):

        remote_script_location = "/home/sshuser/eve_clement/process_script.py"

        # Command has two parameters : location of script on cluster and list of files to process separated by ':'
        cmd_to_execute = "spark-submit --master yarn %(script_location)s %(filename)s" % \
                         {"script_location": remote_script_location, "filename": ":".join(files)}

        data = {"user": "sshuser",
                "host": "groscluster-ssh.azurehdinsight.net",
                "command": cmd_to_execute}

        command = 'ssh {user}@{host} "{command}"'
        os.system(command.format(**data))

        return True


    def process(self, files):

        # Process file and save to azure. move file to processed dir
        logs = self._process_on_cluster_and_save(files)
        for f in files:
            self._write_log(file=f, message='PROCESSED')
            self.processed_list.append(f)

        # TODO :
        # for log in logs:
        #     if log['status'] == 'ok':
        #         # Process done, add file to processed list
        #         self._write_log(file=log['file'], message='PROCESSED')
        #         self.processed_list.append(log['file'])
        #
        #     else:
        #         # Max retry number reached. Add file to error list
        #         self._write_log(file=log['file'], message='ERROR')
        #         self.error_list.append(log['file'])

        return


    def list_files(self):
        files_list = self._block_blob_service.list_blob_names(container_name=self._container, prefix=self._data_dir)

        not_to_process = self.processed_list + self.error_list
        to_process = [file for file in files_list if file not in not_to_process]

        return to_process


    def save(self):
        # Save log to azure
        if len (self._logfile) > 0:
            self._block_blob_service.create_blob_from_text(container_name=self._container,
                                                           blob_name=self._log_path,
                                                           text=self._logfile)
        # Save processed data list
        if len(self.processed_list) > 0:
            formatted_processed_list = ";".join(self.processed_list)
            self._block_blob_service.create_blob_from_text(container_name=self._container,
                                                           blob_name=self._processed_list_path,
                                                           text=formatted_processed_list)
        # Save errors data list
        if len(self.error_list) > 0:
            formatted_error_list = ";".join(self.error_list)
            self._block_blob_service.create_blob_from_text(container_name=self._container,
                                                           blob_name=self._error_list_path,
                                                           text=formatted_error_list)
            return


    def run(self):

        files = self.list_files()
        if len(files) > 0:
            print("Start process")
            self.process(files)
            print("Clean")
            self.clean_container()
            print("save")
            self.save()

        return len(files)

