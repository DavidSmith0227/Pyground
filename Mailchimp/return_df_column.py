import pandas as pd
import re
import pprint
import participant_communication.ppt_repo as ppt_repo
import configFolder as config
import pyodbc


def update_df_header_to_merge_tags(df: pd.DataFrame):
    conn = pyodbc.connect('Trusted_Connection=yes', driver='{SQL Server}', server='SJL-3H1J9H2',
                          database='ProdMailCommunicationsTEST')
    cursor = conn.cursor()
    sql_str = (
        'SELECT [ColumnMapName] ,[mailchimpMergeTag] FROM [ProdMailCommunicationsTEST].[dbo].[ColumnMapping_w_mailchimpMergeTagColumnMapping]  WHERE mailchimpMergeTag IS NOT NULL')
    cursor.execute(sql_str)
    sql_result_list = cursor.fetchall()
    # df = pd.read_csv(r'C:\Users\davsmi\Documents\Python\Davids Laboratory_stage\TestData_stage\TEST_MultiClient_ACHTransitionMailFile_CLDS-0227-07-21_final.csv')
    merge_updated = []
    required_list = ["email_address", "MCID", "FNAME", "LNAME", "MCLIENT", "MHOURS", "MPHONE"]
    for column in df.columns:
        for column_name, merge_tag in sql_result_list:
            if column.lower() == column_name.lower():
                print(f'YAY: {column}=={column_name}')
                df = df.rename(columns={column: merge_tag})
                merge_updated.append(f'{merge_tag}')
    for merge in required_list:
        if merge not in merge_updated:
            print(f'Missing Required Merge Field: {merge}')
    return df


def return_df_column_list_search(df: pd.DataFrame, search_list: list):
    """
        # TODO: add filename
        :param df: pd.DataFrame
        :param search_list:
        :return:
    """
    df_column_return_dict = {
            """ Required """
            "email_address": None,
            "MCID": None,
            "FNAME": None,
            "LNAME": None,
            "MCLIENT": None,
            "MHOURS": None,
            "MPHONE": None,

            """ Optional """                 
            "EDATE": None,
            "MURL": None,
            "PLANTYPENA": None,
            "SEGMENT": None
                             }

    for key in df_column_return_dict:
        for search_str in search_list:
            column_id = return_df_column_str_search(df, search_str)
            df_column_return_dict[key] = column_id
    return df_column_return_dict


def return_df_column_str_search(df: pd.DataFrame, search_str: str):
    """
        returns first column id found from search_string
        # TODO: Wildcard search w/ import re?
        :param df: : pd.DataFrame
        :param search_str: str
        :return: column_int: int
    """
    # for column in df.columns:
    #     if search_str.lower() in column.lower():
    #         column_int = df.columns.get_loc(column)
    #         return column_int

    regex = re.compile(fr'\A(.*)({search_str})(\d*).(.*)\z/i')
    # regex = re.compile(fr'/((?){search_str})')
    compile_str = f'.{search_str}.'
    regex = re.compile(compile_str)
    df_columns = df.columns
    matches = [column for column in df_columns if re.match(regex, column)]
    print(matches)


def file_to_DF():
    location = r'C:\Users\davsmi\Documents\Python\Davids Laboratory_stage\TestData_stage\TEST_MultiClient_ACHTransitionMailFile_CLDS-0227-07-21_final.csv'
    df = pd.read_csv(location, sep=',', dtype=str)
    return df


if __name__ == '__main__':
    print(' *** TEST ***')
    search_str = 'coordinated'
    """ search_list = [email_address, MCID, etc] for Mailchimp Merge Fields """
    search_list = ['emailAddress', 'customerPersonId', 'firstname', 'lastname', 'businessHours', 'PhoneNumber', 'organization', 'businessHours']
    df_r = file_to_DF()
    # str_return = return_df_column_str_search(df_r, search_str)
    # print(str_return)
    # list_return = return_df_column_list_search(df_r, search_list)
    # pprint.pprint(list_return)

    updated_df = update_df_header_to_merge_tags(df_r)
    pprint.pprint(updated_df)

