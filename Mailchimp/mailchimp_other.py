import os
import pandas as pd
import numpy as np
import configFolder.config as config
import repo.return_last_month_year as rlmy
from icecream import ic


def remove_email_from_asset_fields(df: pd.DataFrame):
    """
        removes ["-EMAIL", "EML_"] from asset columns
        :param df: pd.DataFrame
        :return: df: pd.DataFrame
    """
    email_identifiers = "-EMAIL", "EML_"
    asset_cols = ["asset1", "asset2", "asset3", "asset4", "asset5"]
    df = df.replace(np.nan, '', regex=True)
    for id in email_identifiers:
        for col in asset_cols:
            df.update(pd.Series(df[col]).str.replace(id, ''))
    return df


def final_print_file_collation(filename, unsubs_and_nopens):
    """
        grabs the related invalid, and print files from the zerobounce scripts and merges them
        together to form the final file
        :param filename:
        :param unsubs_and_nopens:
        :return:
    """
    print('*** cid_file_record_retrieval ***')

    # #
    file_dir = config.ifp_file_locations["zerobounce_return"]
    unsubs_and_nopens_file = pd.DataFrame(data=None)
    for file in os.scandir(file_dir):
        name = str(file)
        if name == filename:
            unsubs_and_nopens_file = pd.read_csv(file)
            unsubs_and_nopens_file = unsubs_and_nopens_file[unsubs_and_nopens_file.customerPersonId.isin(unsubs_and_nopens)]
    for file in os.scandir(file_dir):
        name = str(file)
        if name.find(filename) > 0 and name.find('_print.csv') > 0:
            print_file = pd.read_csv(file)
    for file in os.scandir(file_dir):
        name = str(file)
        if name.find(filename) > 0 and name.find('invalid') > 0:
            invalid_file = pd.read_csv(file)
    print_file = pd.read_csv(str(fr'{file_dir}\{filename}_print.csv'))
    print_file_concat = pd.concat([print_file, unsubs_and_nopens_file, invalid_file])
    print_final_file = remove_email_from_asset_fields(print_file_concat)
    print_final_save_loc = str(fr'{config.ifp_file_locations["sunprint_ftp"]}\{filename}_print_final.csv')
    print_final_file.to_csv(print_final_save_loc, index=False)
    return print_final_save_loc


def make_tag_name(tag_name):
    """
        formats automated tag_name with next next_month's dates 'Alfred_{next_months_year}_{next_month}_{tag_name}'
        :param tag_name:
        :return: tag_name formatted
    """
    next_months_year = rlmy.return_year_of_next_month()
    next_month = rlmy.return_next_month()
    tag_title = str(f'Alfred_{next_months_year}_{next_month}_{tag_name}')
    return tag_title


if __name__ == '__main__':
    print(f'************\n****TEST****\n************\n\n')
    # non_openers = ['19101370', '15620119']
    # final_print_file_creation('IFP_OngoingAgeInMailFile__TEST__CLDS-20127_2021-06-15_HRA', non_openers)

    test_file_location = str(r'C:\Users\davsmi\Documents\Python\Davids Laboratory\Mailchimp\TestData\Mailchimp_IFP_add_test.csv')
    df = pd.read_csv(test_file_location)
    updated_df = remove_email_from_asset_fields(df)
    ic(updated_df)
