from icecream import ic
import configFolder.config as config
import repo.dataSftpConnect as dataSftpConnect
from pyxlsb import open_workbook
import paramiko
from paramiko import client as pclient
import pyodbc
import csv
import os
import datetime
from datetime import date

import numpy as np
import pandas as pd

import repo.sql_import

""" Config and constants """
ic.enable()
# ic.disable()
host = config.dataSFTP['host']
port = 22
username = config.dataSFTP['username']
password = config.dataSFTP['password']
""" /Config and constants """

    # TODO:
    #  0. Research "from rich import print, pretty, inspec"
    #  DONE python_sftp_connect
    #  DONE python_log_file
    #  DONE Update [ProdMailCommunications].[dbo].[qualityAssuranceProcessedLog]
    #  DONE python_delete_file
    #  5. test
    #  6. Add to Alfred
    #  7. Create Sproc for "Bad Address search (by asset?)"


def return_list_of_logged_files():
    table = config.sql_prodMail['QA_processingTable']
    db = config.sql_prodMail['database']
    conn = pyodbc.connect('Trusted_Connection=yes', driver='{SQL Server}', server='ProdSqlFCI', database=db)
    sql_select = f'SELECT * FROM {table} ORDER BY QA_dateProcessed DESC'
    df_sql = pd.read_sql_query(sql_select, conn)
    return df_sql


def delete_old_or_null_files_from_SFTP(directory, filenames, size=934):
    today = date.today()
    month_ago = str.replace((today - datetime.timedelta(days=30)).strftime('%Y-%m-%d'), '-', '')
    print(month_ago)
    with paramiko.Transport((host, port)) as transport:
        transport.connect(username=username, password=password)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            for file in sftp.listdir_attr(directory):
                last_modified_date = str.replace(datetime.datetime.fromtimestamp(file.st_mtime).strftime('%Y-%m-%d'), '-', '')
                print(f'{int(last_modified_date)} <= {int(month_ago)}')
                if file.st_size <= size and file.filename in filenames and int(last_modified_date) <= int(month_ago):
                    print(f'remove: *** {file}')
                    # sftp.remove(file)
                else:
                    continue


def get_passed_file_names(file_log_details):
    filenames = []
    for detail in file_log_details:
        if detail["passed"] == 1:
            filenames.append(detail["filename"])
    return filenames


def log_qa_file(file_log_details):
    for detail_log in file_log_details:
        filename = detail_log["filename"]
        passed = detail_log["passed"]
        details = detail_log["details"]
        quantity = detail_log["quantity"]
        repo.sql_import.sqlImport_ProcessingLog(filename, passed, details, quantity)


def get_file_details(files):
    if isinstance(files, list):
        downloaded_files = files
    else:
        downloaded_files = [files]
    file_log_details = []
    for file in downloaded_files:
        filename = os.path.basename(file)
        file_df = pd.read_csv(file)
        if file_df.empty is True:
            quantity = 0
            details = 'No-Records'
            passed = 1
        else:
            quantity = file_df.count(axis=0)
            details = 'returned-file'
            passed = 0

        file_log_details.append({'filename': filename, 'passed': passed, 'details': details, 'quantity': quantity})
    return file_log_details


def download_expected_blank_USPS_files(directory, size=934):
    downloaded_files = []
    with paramiko.Transport((host, port)) as transport:
        transport.connect(username=username, password=password)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            for file in sftp.listdir_attr(directory):
                print(f'file.filename: {file.filename}')
                if file.st_size <= size:
                    filepath = fr'{directory}\{file.filename}'
                    save_path = f'{config.dataSFTP["usps_check_folder"]}\{file.filename}'
                    downloaded_files.append(save_path)
                    print(f'filepath: {filepath}')
                    print(f'save_path: {save_path}')
                    sftp.get(filepath, save_path)
    return downloaded_files


def connect_sun_sftp(directory=None):
    """ Cannot iterate through this function if it's defined as a variable. """
    with paramiko.Transport((host, port)) as transport:
        transport.connect(username=username, password=password)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            for file in sftp.listdir_attr(directory):
                ic(file)
    return sftp


def run():
    # TODO move functions to "dataSftpConnect.py"
    directory = config.directory["sftp_nonusps"]
    downloaded_files = download_expected_blank_USPS_files(directory)
    file_log_details = get_file_details(downloaded_files)
    log_qa_file(file_log_details)
    filenames = get_passed_file_names(file_log_details)
    delete_old_or_null_files_from_SFTP(directory, filenames)
    # delete_old_or_null_files(directory)

    # TODO: disconnect paramiko


if __name__ == '__main__':
    run()
    print('************\n****TEST*****\n***********')
    # directory = config.directory["sftp_nonusps"]
    # download_file_test = r'C:\Users\davsmi\Documents\Python\Davids Laboratory\USPS_QA\USPS_check_folder\312358_HRA_FINAL_312358_20210701-100_07012021_BadAddresses.txt'
    # file_log_details = get_file_details(r'C:\Users\davsmi\Documents\Python\Davids Laboratory\USPS_QA\USPS_check_folder\308636_HRA_FINAL_308636_20210201-000_02012021_BadAddresses.txt')
    # log_qa_file(file_log_details)
    # delete_old_or_null_files_from_SFTP(directory)