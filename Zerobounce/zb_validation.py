import os
import math
import time
import shutil
import logging
import pandas as pd
from datetime import date
import repo.send_email as eml
import repo.bump_google as bg
import repo.column_mapping as cm
import configFolder.config as config
from zerobouncesdk import ZeroBounce, ZBException, ZBApiException


def zb_connect():
    key = config.zerobounce["api_key"]
    try:
        zerobounce = ZeroBounce(key)
        return zerobounce
    except ZBApiException as error:
        logging.info("get_file error message: " + str(error))
    # except ZBMissingApiKeyException as error:
    #     logging.info(ic("get_file error message: " + str(error)))


def zb_sufficient_credit_check(file_location: str) -> bool:
    """
    if file is larger than we have current credits for, return False to stop process
    :param file_location:
    :return:
    """
    df = pd.read_csv(file_location, encoding='cp1252')
    file_count = df.shape[0]
    zerobounce = zb_connect()
    max_try = 3
    for i in range(max_try):
        try:
            zb_credit_count = zerobounce.get_credits().credits
            logging.info(f'{zb_credit_count} credits available. {file_count}, needed.')
            if zb_credit_count is not None and file_count > int(zb_credit_count):
                subject = fr'Insufficient ZeroBounce remaining Balance for file: {file_location}'
                msg = (fr'Good Evening, <br>'
                       f'This email is to inform you of a low remaining balance of ZeroBounce tokens.<br><br>'
                       f'REMAIMING BALANCE: {zb_credit_count}<br>'
                       f'Date:              {date.today()}<br>'
                       f'file_name: {file_location}<br>'
                       f'file_count: {file_count}')
                recipients = config.email['test']
                eml.email(recipients, subject, msg)
                return False
            elif file_count < int(zb_credit_count):
                logging.debug(f'True: {file_count}; available: {int(zb_credit_count)}')
                return True
            elif zb_credit_count is None:
                continue
        except Exception:
            bg.wake_google(url='https://www.zerobounce.net/')
            continue


def zb_file_status(file_id):
    logging.debug('*** zb_file_status ***')
    zerobounce = zb_connect()
    try:
        response = zerobounce.file_status(file_id)
        response = response.__dict__["file_status"]
        return response
    except ZBApiException as error:
        status = f"file_status error message: {str(error)}"
        logging.info(status)
    # except ZBMissingApiKeyException as error:
    #     status = f"file_status error message: {str(error)}"
    #     logging.info(ic(status))


def zb_get_file(dst_directory: str, original_file_name_path: str, file_id: str) -> list:
    """
    Attempts to get ZB processed file for 20 tries.
    :param original_file_name_path:  original file name
    :param file_id:
    :param dst_directory:
    :return: processed_file: if ZB doesn't time out, list will be [True, file name and path]
    if it does time out it will be [False, file_id]
    """
    status = f"Getting ZB results based on file id {file_id}"
    logging.info(status)

    zerobounce = zb_connect()
    success_check = zb_file_status(file_id)
    attempts = 0

    while success_check != 'Complete' and attempts < 20:
        time.sleep((attempts + 15) * 5)
        file_status_response = zerobounce.file_status(file_id)
        success_check = file_status_response.file_status
        attempts += 1
        logging.debug(f'Attempt number:{attempts}; {success_check}')
        if success_check is None:
            bg.wake_google(url='https://www.zerobounce.net/')
    if success_check == 'Complete':
        status = f"Batch finished after #{attempts} attempts  --proceeding"
        logging.info(status)
        file_basename = os.path.basename(original_file_name_path)
        return_file_name = f"{file_basename.split('.')[0]}_zb_results.csv"
        dst_file_and_path = os.path.join(dst_directory, return_file_name)
        dst_file_and_path = dst_file_and_path.replace('\\', '/')
        try:
            response = zerobounce.get_file(file_id, dst_file_and_path)
            processed_file = response.local_file_path
            """ save duplicate file for DB tracking """
            file_archive_dir = os.path.join(config.shared_directory["email_validation_dir"], 'Archive/')
            file_archive_path = os.path.join(file_archive_dir, return_file_name)
            shutil.copy(processed_file, file_archive_path)

            status = f'ZB file results posted to: {processed_file}, and to {file_archive_path}'
            logging.info(status)
            return [True, processed_file]
        except ZBApiException as error:
            status = f"get_file error message:  {str(error)}"
            logging.info(status)
        # except ZBMissingApiKeyException as error:
        #     status = f"get_file error message:  {str(error)}"
        #     logging.info(ic(status))
    elif success_check != 'Complete':
        return [False, file_id]


def zb_send_file(file_name_path: str) -> str:
    """
    Posts email file to Zero Bounce for processing
    :param file_name_path: file and path to process name
    :return: ZB file id of file being processed.
    """
    col_dict = cm.map_file_for_mc_processing(file_name_path)
    email_index = col_dict['EMAIL'] + 1
    print(email_index)
    zerobounce = zb_connect()
    try:
        # response = zerobouncesdk.send_file(
        response = zerobounce.send_file(
            file_path=file_name_path,
            email_address_column=email_index,
            return_url=None,
            first_name_column=None,
            last_name_column=None,
            gender_column=None,
            ip_address_column=None,
            has_header_row=True
        )
        logging.info(f"ZeroBounce send_file: {str(response.file_id)}")
        if response.success is True:
            return response.file_id
    except ZBException as e:
        logging.info(f"ZeroBounce error message: {str(e)}")
    # except ZBMissingApiKeyException as error:
    #     logging.info(ic(f"get_credits error message:  {str(error)}"))
    except Exception as e:
        logging.info(f"zb_validation.send_file Failed for reason: {e}")


def pause_for_zb_processing(file_name_path: str):
    """
    Sets up a delay to allow Zerobounce time to process this is estimated on file count and a minimum of 10 min
    :param file_name_path: file and path
    :return: None
    """
    seconds_to_process = estimated_processing_time(file_name_path)
    minimum_processing_time = 20
    if seconds_to_process > minimum_processing_time:
        minimum_processing_time = seconds_to_process
    status = f"Waiting for a minimum of {minimum_processing_time} seconds to allow Zerobounce to process..."
    logging.debug(status)
    time.sleep(minimum_processing_time)
    pass


def estimated_processing_time(file_path: str) -> int:
    """
    Calculates estimated processing time based on average of 1-3 hours per 100k; average of about 17 per second.
    allow for 15 min minimum
    :param file_path: file and path
    :return:
    """
    df = pd.read_csv(file_path, encoding='cp1252')
    record_count = df.shape[0]
    return math.ceil(record_count/17)


def run(obj_directory: str, file_name_path: str, sent_file_id: str = None) -> list:
    """
    If a sent_file_id is provided, this will check ZB for file status. If not it will attempt to process.
    :param sent_file_id: Id of file currently processing in ZB, if applicable
    :param obj_directory: file directory, used to post results
    :param file_name_path: file and path to process name
    :return: zb_file_results: [bool:file and path of ZB results]
    """

    if sent_file_id is None:
        logging.debug('no previous sent file submitted for {file_name_path.')
        zb_credit_check = zb_sufficient_credit_check(file_name_path)
        if zb_credit_check:
            sent_file_id = zb_send_file(file_name_path)
            pause_for_zb_processing(file_name_path)
        else:
            logging.info('Insufficient credits in Zerobounce.')
    zb_file_results = zb_get_file(obj_directory, file_name_path, sent_file_id)
    return zb_file_results


if __name__ == '__main__':
    # THIS IS GOLD; https://pypi.org/project/zerobouncesdk/

    directory = r'\\fileshare\misc\DataFiles'
    file_name = 'PDP_disruption_NonOpers_CDFR-10657_20240805.csv'
    test_file = os.path.join(directory, file_name)

    run_response = run(directory, test_file, sent_file_id=None)

    send_file_id = zb_send_file(test_file)
    print(send_file_id)
    # send_file_id = '67a07f6d-7502-4d9b-a849-b2dec36674a6'
    # get_file_response = zb_get_file(directory, file_name, send_file_id)

    pass

