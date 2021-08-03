from zerobouncesdk import zerobouncesdk, ZBApiException, ZBMissingApiKeyException
import pandas as pd
import configFolder.config as config
import repo.send_email as eml
from datetime import date
import numpy as np
import shutil
import time
import os


""" connections & constants """
# key = config.zerobounce["api_key"]
# try:
#     client = zerobouncesdk.initialize(key)
# except ZBApiException as error:
#     print("get_file error message: " + str(error))
# except ZBMissingApiKeyException as error:
#     print("get_file error message: " + str(error))
""" /connections & constants """


def zb_get_credits():
    """
        looks at remaining credits at ZeroBounce and sends notifying email when running low
        :return: None
    """
    print('\n*** zb_get_credits ***')
    zb_connect()
    try:
        response = zerobouncesdk.get_credits()
        print("get_credits success response: " + str(response))
    except ZBApiException as error:
        print("get_credits error message: " + str(error))
    except ZBMissingApiKeyException as error:
        print("get_credits error message: " + str(error))
    # Logging('ZB credits remaining: ' + response.credits)
    print('ZB credits remaining: ' + response.credits)
    if int(response.credits) < 100000:
        subject = f'ZeroBounce remaining Balance: {response.credits}'
        msg = (fr'Good Evening Master Suwayneski,' + '\n' +
               f'This email is to inform you of a low remaining balance of ZeroBounce tokens.' + '\n' +
               '\n' +
               f'REMAIMING BALANCE: {response.credits}' + '\n' +
               f'Date:              {date.today()}')
        recipients = ["nathvirg@extendhealth.com; jersuwi@extendhealth.com"]
        # TODO: update email line when approved.
        eml.email(recipients, subject, msg)


def zb_file_status(file_id):
    print('*** zb_file_status ***')
    try:
        response = zerobouncesdk.file_status(file_id)
        response = response.__dict__["file_status"]
        # print(response)
    except ZBApiException as error:
        print("file_status error message: " + str(error))
    except ZBMissingApiKeyException as error:
        print("file_status error message: " + str(error))
    return response


def zb_get_api_usage():
    """
        no current use case for this...
        :return: None
    """
    try:
        start_date = date(2019, 7, 5)
        end_date = date(2019, 7, 15)
        response = zerobouncesdk.get_api_usage(start_date, end_date)
        print("get_api_usage success response: " + str(response))
    except ZBApiException as error:
        print("get_api_usage error message: " + str(error))
    except ZBMissingApiKeyException as error:
        print("get_api_usage error message: " + str(error))


def zb_email_splitter(localFilePath, filename):
    print('\n*** zb_email_splitter ***')
    df_raw = pd.read_csv(localFilePath)
    df_valid = pd.DataFrame(data=None, columns=df_raw.columns)
    df_invalid = pd.DataFrame(data=None, columns=df_raw.columns)
    directory = config.ifp_file_locations["zerobounce_return"]

    df_valid = df_valid.append(df_raw.loc[(df_raw["ZB Status"] == 'valid') | (df_raw["ZB Status"] == 'catch-all')])
    df_invalid = df_invalid.append(df_raw.loc[(df_raw["ZB Status"] != 'valid') & (df_raw["ZB Status"] != 'catch-all')])

    """ Removing ZB columns for Mailchimp / mail file merge. """
    x = list(df_valid.columns[-12:])
    for column in x:
        df_valid = df_valid.drop(column, axis=1)
        df_invalid = df_invalid.drop(column, axis=1)
    valid_loc = str(fr'{directory}\{filename}_VALID.csv')
    df_valid.to_csv(valid_loc, ",", index=False)
    invalid_loc = str(fr'{directory}\{filename}_invalid.csv')
    df_invalid.to_csv(invalid_loc, ",", index=False)

    # return {"valid_loc": valid_loc, "invalid_loc": invalid_loc}
    return valid_loc


def zb_get_file(batch_fileid, filename: str):
    """
        downloads return file from a batch file sent to ZeroBounce
        :param batch_fileid:
        :param filename: base file name, downloaded file will concat(filename, '_RAW.csv')
        :return: downloaded file path
    """
    print('\n*** zb_get_file ***')
    print('sleeping for 5 min for file to process')
    zb_connect()
    # time.sleep(300)

    zb_path = config.ifp_file_locations["zerobounce_return"]
    save_file_path = str(f'{zb_path}{filename}_RAW.csv')
    success_check = zb_file_status(batch_fileid)
    attempts = 1
    while success_check != 'Complete' and attempts < 30:
        time.sleep(attempts * 2)
        success_check = zb_file_status(batch_fileid)
        attempts += 1
    if success_check == 'Complete':
        print(f'Batch finished after #{attempts} attempts  --proceeding')
        # logging(response['response_body_url'])
        try:
            response = zerobouncesdk.get_file(batch_fileid, save_file_path)
            print("get_file success response: " + str(response))
        except ZBApiException as error:
            print("get_file error message: " + str(error))
        except ZBMissingApiKeyException as error:
            print("get_file error message: " + str(error))
        # print(response.__dict__["localFilePath"])
        return response.__dict__["localFilePath"]


def zb_send_file(file_location, address_column: int = 16):
    """
        Sends file to Zerobounce for Email verification
        :param file_location:  file location to send
        :param address_column: column number for Email Address (index starts at 1, not 0)
                '''IFP File format's address column is 16, setting that as default for now.'''
        :return: file_id
    """
    print('\n*** zb_send_file ***')
    zb_connect()
    try:
        response = zerobouncesdk.send_file(
            file_path=file_location,
            email_address_column=address_column,
            has_header_row=True
            )
        print("sendfile success response: " + str(response))
    except ZBApiException as error:
        print("sendfile error message: " + str(error))
    except ZBMissingApiKeyException as error:
        print("get_credits error message: " + str(error))
    print(response.file_id)
    return str(response.file_id)


def zb_split_email_rows_from_file(local_file_path, filename: str) -> dict:
    """
        :param local_file_path: file to split
        :param filename: str: used for save names
        :returns: dict{"email_file_loc", "print_file_loc"}
    """
    print('\n*** zb_split_email_rows_from_file ***')
    df_full = pd.read_csv(local_file_path)
    df = df_full.replace(np.nan, '', regex=True)
    emails_df = pd.DataFrame(data=None, columns=df.columns)
    print_df = pd.DataFrame(data=None, columns=df.columns)

    column_numbers = 46, 47, 48, 49, 50
    asset_columns = 'asset1', 'asset2', 'asset3', 'asset4', 'asset5'
    email_identifiers = "EMAIL", "EML_"

    emails_list = []
    for id in email_identifiers:
        email_df = df.loc[df['asset1'].str.contains(id, case=False) |
                          df['asset2'].str.contains(id, case=False) |
                          df['asset3'].str.contains(id, case=False) |
                          df['asset4'].str.contains(id, case=False) |
                          df['asset5'].str.contains(id, case=False)]
        emails_list.append(email_df)

    emails_df = pd.concat(emails_list)
    emails_df = emails_df.drop_duplicates(keep='first')
    drop_cids = emails_df["customerPersonId"].to_list()
    print_df = df[~df["customerPersonId"].isin(drop_cids)]

    save_file_name = str.replace((str.replace(filename, '.csv', '')), 'txt', '')
    directory = str('\\'.join(local_file_path.split('\\')[0:-1]))

    email_loc = str(fr'{directory}\{save_file_name}_email.csv')
    print_loc = str(fr'{directory}\{save_file_name}_print.csv')
    emails_df.to_csv(email_loc, sep=',', index=False)
    print_df.to_csv(print_loc, sep=',', index=False)
    print(f'files separated between Email and Print in directory:\n---->{directory}')
    return {"email_file_loc": email_loc, "print_file_loc": print_loc}


def find_file(mailing_type: str) -> dict:
    """
        finds IFP files in Data Files shared drive and moves it to an archive.
        :param mailing_type: str
        :returns dict: {'filename', 'file_directory'}
    """
    print('*** find_ifp_file ***')
    directory = config.ifp_file_locations["datafiles"]
    for file in os.scandir(directory):
        if file.is_file():
            filename = str(file)
            if filename.find(mailing_type) > 0:
                if filename.find('.csv') or filename.find('.txt') > 0:
                    file_directory = os.fspath(file)
                    filename = str.split(filename, sep="'")
                    filename = filename[1]
                    save_loc = fr'{config.ifp_file_locations["datafilesprocessed"]}\{filename}'
                    # TODO: Copy only on Test
                    # shutil.move(file, save_loc)
                    shutil.copy(file, save_loc)
                    return {'filename': filename, 'file_directory': save_loc}


def zb_connect():
    key = config.zerobounce["api_key"]
    try:
        client = zerobouncesdk.initialize(key)
    except ZBApiException as error:
        print("get_file error message: " + str(error))
    except ZBMissingApiKeyException as error:
        print("get_file error message: " + str(error))
    return client


if __name__ == '__main__':
    print('************\n****test\n************')

    # zb_send_file(
    # r'C:\Users\davsmi\Documents\Python\Davids Laboratory\Mailchimp\TestData\IFP-OngoingMonthly_CLDS-14965_20200427_TEST_SMALL_EMAIL.csv')

    # zb_get_credits()

    # zb_file_status()

    # zb_delete_file()

    # zb_get_api_usage()

    # zb_get_file('febe5d28-e798-473c-a611-c766a6633fc0', 'IFP_OngoingAgeInMailFile_CLDS-20127_2021-06-15_HRA')

    # zb = zb_email_splitter(r'C:\Users\davsmi\Documents\Python\Davids Laboratory\Mailchimp\TestData\zeroBounce return files\IFP_OngoingAgeInMailFile_CLDS-20127_2021-06-15_HRA_RAW.csv', 'IFP_OngoingAgeInMailFile_CLDS-20127_2021-06-15_HRA_RAW.csv')
    # print(zb["raw_loc"])
    # print(zb["valid_loc"])
    # print(zb["invalid_loc"])

    # success_check = zb_file_status('ca937404-fd2e-4c1c-9a3a-5f64e7655a2b')

    # response = zb_file_status('ca937404-fd2e-4c1c-9a3a-5f64e7655a2b')

    # zb_split_email_rows_from_file(
    #     r'C:\Users\davsmi\Documents\Python\Davids Laboratory\Mailchimp\TestData\IFP_OngoingAgeInMailFile__TEST__CLDS-20127_2021-06-15_HRA.csv',
    #     'IFP_OngoingAgeInMailFile__TEST__CLDS-20127_2021-06-15_HRA_RAW.csv')
