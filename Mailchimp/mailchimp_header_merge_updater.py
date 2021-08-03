import pyodbc
import pandas as pd

# TODO: update to your Local SQL DB


def update_df_header_to_merge_tags(df):
    """
    :param file_loc: str | pd.DataFrame
    :return: Save_file_location:
    """
    conn = pyodbc.connect('Trusted_Connection=yes', driver='{SQL Server}', server='SJL-3H1J9H2',
                          database='ProdMailCommunicationsTEST')
    cursor = conn.cursor()
    sql_str = (
        'SELECT [ColumnName], [ColumnMapName] FROM [ProdMailCommunicationsTEST].[dbo].[ColumnMapping_w_mailchimpMergeTagColumnMapping]  WHERE mailchimpMergeTag IS NOT NULL')
    # for merge Tag - replace "column_name" with ', [mailchimpMergeTag]'
    cursor.execute(sql_str)
    sql_result_list = cursor.fetchall()
    merge_updated = []
    required_list = ["emailaddress", "personid", "firstname",  "lastname", "clientname",  "businesshours", "clientcallcenterphonenumber"]
    """ ^ ColumnName  V Merge_tags"""
    # required_list = ["email_address", "MCID", "FNAME", "LNAME", "MCLIENT", "MHOURS", "MPHONE"] #
    if isinstance(df, str) is True:
        df = pd.read_csv(df)
    for column in df.columns:
        for column_name, ColumnMapName in sql_result_list:
            if column.lower() == ColumnMapName.lower():
                print(f'YAY: {column}=={ColumnMapName}')
                df = df.rename(columns={column: column_name})
                merge_updated.append(f'{column_name}')
    for merge in required_list:
        if merge not in merge_updated:
            # THROW ERROR HERE
            print(f'Missing Required Merge Field: {merge}')
    save_file_loc = r'C:\Users\davsmi\Documents\Python\Davids Laboratory_stage\TestData_stage\Original data\merge_file_test.csv'
    df.to_csv(save_file_loc, index=False)

    return save_file_loc


if __name__ == "__MAIN__":
    file_loc = ''
    update_df_header_to_merge_tags(file_loc)