import os
import time
from azure.storage.blob import BlockBlobService
import csv
from pathlib import Path

'''
Connection Parameters
'''
PARAMS = {'account_name': 'storagealternance',
          'account_key': 'ziygUeiiiGZElpbMZEYBo3r5YjUFuryyvSB8wq3UMpsJWl9hWdjDxnvWBin+EHcuhB+KEd2G3C33dGX+YifCMg==',
          'container_name': 'clementeve',
          'folder': 'datas/',
          'input_path': '/home/sshuser/data/',
          'parts_path': '/home/sshuser/data/temp'
          }

DIR_SUFFIX = '/sent'
DIR_SENT_FILES = str(Path(PARAMS['input_path']).parent) + DIR_SUFFIX
EXT = ('.csv', '.csv.part', '.parquet')

BLOCK_BLOB_SERVICE = BlockBlobService(
    account_name=PARAMS['account_name'], account_key=PARAMS['account_key'])

'''
Functions to get Datas & Send them to blob storage
'''
def get_data_to_send(path):
    """
    Function that returns a csv file
    """
    files_to_send = []
    for root, subfolders, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith(EXT):
                files_to_send.append(os.path.join(path, filename))
            else:
                pass

    return files_to_send


def send_data(data, part_file=False):
    generator = BLOCK_BLOB_SERVICE.list_blobs(PARAMS['container_name'], prefix=PARAMS['folder'])
    count_blobs = len(list(generator))

    new_name = 'data_' + str(count_blobs) + '.csv'

    if part_file:
        BLOCK_BLOB_SERVICE.create_blob_from_text(container_name=PARAMS['container_name'],
                                                 blob_name=PARAMS['folder'] + new_name,
                                                 text=data)
    else:
        BLOCK_BLOB_SERVICE.create_blob_from_path(container_name=PARAMS['container_name'],
                                                 blob_name=PARAMS['folder'] + new_name,
                                                 file_path=data)

    return


def split(files, delimiter=',', row_limit=1000000, output_path=PARAMS['parts_path'], keep_headers=True):

    output_name_template = files.replace(".csv", "_part_%s.csv")

    filehandler = open(files)
    new_files = []

    reader = csv.reader(filehandler, delimiter=delimiter)
    current_piece = 1
    current_out_path = os.path.join(
        output_path,
        output_name_template % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
    current_limit = row_limit
    if keep_headers:
        headers = next(reader)
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
                output_path,
                output_name_template % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
            new_files.append(current_out_path)
            print("Creates part file: ", current_out_path)

            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)

    return new_files



def gen_chunks(file, chunksize):
    """
    Chunk generator. Take a CSV `reader` and yield
    `chunksize` sized slices.
    """
    reader = csv.reader(open(file, 'r'))
    #header = "100, 300, 400, 500"
    chunk = []
    for index, line in enumerate(reader):
        if index == 0:
            header = line

        if (index % chunksize == 0 and index > 0):
            yield chunk
            del chunk[:]
            chunk.append(header)

        chunk.append(line)
    yield chunk




def already_sent_list():

    if BLOCK_BLOB_SERVICE.exists(PARAMS['container_name'], "processed/processed_local_list.txt"):
        b = BLOCK_BLOB_SERVICE.get_blob_to_text(PARAMS['container_name'], "processed/processed_local_list.txt")
        data_list = b.content.split(";")
    else:
        data_list = []

    return data_list


def save_sent_list(not_to_send):
    # Save processed data list
    if len(not_to_send) > 0:
        formatted_processed_list = ";".join(not_to_send)
        BLOCK_BLOB_SERVICE.create_blob_from_text(container_name=PARAMS['container_name'],
                                                 blob_name="processed/processed_local_list.txt",
                                                 text=formatted_processed_list)




if __name__ == '__main__':
    # EXECUTION
    while True:

        print("START")
        # Fichiers present dans le dir au premier scan avant creation des morceaux
        ALL_DATAS = get_data_to_send(PARAMS['input_path'])
        not_to_send = already_sent_list()

        files_to_send = [file for file in ALL_DATAS if file not in not_to_send]
        for files in files_to_send:
            FileSize = os.stat(files)

            # Si fichier trop gros
            if (FileSize.st_size / (1024 * 1024)) > 1000:
                print(files, FileSize.st_size, 'size is too big, processing...')
                # Genere chunk du fichier et envoie sur azure
                for chunk in gen_chunks(files, chunksize=1000000):
                    # Envoyer chunk sur azure
                    csv_text = "\n".join(",".join(row) for row in chunk)
                    send_data(csv_text, part_file=True)

            else:
                print("Send file:", files)
                send_data(files)

            # Sauvegarde le fait que le fichier a deja ete envoye
            not_to_send.append(files)

        save_sent_list(not_to_send)
        print('process done')
        time.sleep(60)
