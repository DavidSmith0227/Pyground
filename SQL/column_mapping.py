import pandas
import pandas as pd
import pyodbc
import configFolder.config as config
import repo.queries as sql


def read_col_mapping():
    sql_conn = sql.sql_connect(server=config.sql_prodParticipantComm["host"],
                               database=config.sql_prodParticipantComm["database"])
    column_name_mapping_df = pd.read_sql('SELECT columnName, ColumnMapName FROM pc.CommunicationsFileColumnMapping', sql_conn)
    return column_name_mapping_df


def map_file_for_mc_processing(file_path: str, zero_based_offset = 0) -> dict:
    """
    :param file_path: file and path
    :return: dict of column and index
    """
    mc_names = ['MCID', 'FNAME', 'LNAME', 'MCLIENT', 'MURL', 'MHOURS', 'MPHONE', 'EMAIL', 'isprimary', 'VDATE',
                'FUNDLONG', 'FUNDSHORT', 'HRAAMOUNT', 'fundlong', 'fundshort', 'hraamount', 'Street Address', 'Address Line 2', 'City', 'State', 'Zip', 'Country']
    column_mapping_df = read_col_mapping()
    
    # Remove only the specific conflicting hraamount -> hraamt mapping from database
    column_mapping_df = column_mapping_df[~((column_mapping_df['ColumnMapName'] == 'hraamount') & (column_mapping_df['columnName'] == 'hraamt'))]
    
    
    mc_col_mapping_df = column_mapping_df.replace(
        {
            'columnName': {
                'personid': 'MCID',
                'firstname': 'FNAME',
                'lastname': 'LNAME',
                'clientname': 'MCLIENT',
                'clientwebsiteurl': 'MURL',
                'businesshours': 'MHOURS',
                'clientcallcenterphonenumber': 'MPHONE',
                'emailaddress': 'EMAIL',
                'address1': 'Street Address',
                'address2':'Address Line 2',
                'city': 'City',
                'state': 'State',
                'zipcode': 'Zip',
                'country':'Country' 
                }
            }
    )
    mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['VDATE', 'variabledate']
    mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['FUNDLONG', 'subsdyaccountfullname']
    mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['FUNDSHORT', 'subsdyaccountabbreviatedname']
    mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['HRAAMOUNT', 'hraamount']
    
    # Add mappings for the actual database column names
    mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['FUNDLONG', 'fundinglongname']
    mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['FUNDSHORT', 'fundingshortname']
    
    file_df = pd.read_csv(file_path, header=0, encoding='Latin-1')
    file_df.columns = map(str.lower, file_df.columns)
    
    # Add mappings for the household-renamed columns AFTER we know what columns exist
    if 'fundlong' in file_df.columns:
        mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['fundlong', 'FUNDLONG']
    if 'fundshort' in file_df.columns:
        mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['fundshort', 'FUNDSHORT']
    if 'hraamount' in file_df.columns:
        mc_col_mapping_df.loc[len(mc_col_mapping_df.index)] = ['hraamount', 'HRAAMOUNT']
    
    for index, row in mc_col_mapping_df.iterrows():
        file_df.rename(columns={row['ColumnMapName']: row['columnName']}, inplace=True, errors='ignore')
    
    return_dict = {}
    for value in mc_names:
        try:
            index = file_df.columns.get_loc(value)
            return_dict[value] = index + zero_based_offset
        except KeyError:
            return_dict[value] = None

    return_dict['obj_file'] = file_path
    return return_dict


def map_file_headers(processing_df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Generic function to map columns to the standard expectations for column header titles.
    """
    column_mapping_df = read_col_mapping()
    processing_df.columns = map(str.lower, processing_df.columns)

    # perform names for necessary columns
    for index, row in column_mapping_df.iterrows():
        processing_df.rename(columns={row['ColumnMapName']: row['columnName']},
                            inplace=True)
    return processing_df


if __name__ == "__main__":
    test_ = 'Z://MultiClient_ACHTransitionMailList_CDFR-8632_20220502_V3_email_zb_scrubbed_hse.csv'
    x= map_file_for_mc_processing(test_)
    pass
