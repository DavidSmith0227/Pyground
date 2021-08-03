import configFolder.config as config
import repo.smartsheet.return_sheetid as rs
import smartsheet
import csv
import datetime
import logging


def return_ss_ids(ss, shared_dirve_letter):
    all_sheet_ids = []
    ss_id_config = shared_dirve_letter + config.directory['smartsheet_backup_list']
    with open(ss_id_config,'r')as obj_file:
        reader = csv.reader(obj_file)
        next(reader)
        for row in reader:
            logging.info(row[0]+ ' request to backup.')
            sheet_id = rs.get_sheetid(ss, row[0])
            all_sheet_ids.append(sheet_id)
    return all_sheet_ids

def backup_smartsheet():
    token = config.smartsheet['accessToken']
    shared_dirve_letter = config.directory['print_comm_drive_letter']

    try:
        print('smartsheet_api_defs.backup.py started')
        logging.info('smartsheet_api_defs.backup.py started')
        ss = smartsheet.Smartsheet(token)
        backup_archive = shared_dirve_letter + config.directory['smartsheet_backup_archive']
        sheet_ids = return_ss_ids(ss, shared_dirve_letter)
        for id in sheet_ids:
            ss.Sheets.get_sheet_as_excel(id, backup_archive, alternate_file_name=None)
            print(f'Sheet id {id} archived to {backup_archive}')
            logging.info(f'Sheet id {id} archived to {backup_archive}')
        logging.info('Complete')
    except Exception:
        logging.exception('An ERROR has occurred')


if __name__=='__main__':
    token = config.smartsheet['accessToken']
    log_file_name = 'smartsheet_backup_' + datetime.datetime.now().strftime("%Y%m%d") + '.txt'
    shared_dirve_letter = config.directory['print_comm_drive_letter']
    log_dir = shared_dirve_letter + config.directory['log_directory']
    logging.basicConfig(level=logging.INFO, filename=log_dir + log_file_name, filemode='a'
                        , format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%y %H:%M:%S')
    backup_smartsheet()
