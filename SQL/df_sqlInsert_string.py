import numpy as np
import pandas as pd
import logging
from icecream import ic


pd_sql_dtype_map = {
    'int32': 'INT',
    'int64': 'BIGINT',
    'object': 'VARCHAR(300)',
    'float64': 'VARCHAR(300)',
    'bool': 'bit'
    }


def run(df:pd.DataFrame, table_name: str = '##configinsert', sql_insert_where: str = '') -> str:
    """
    provides a sql script for the provided dataframe to be inserted into the tableName provided.
    :param: table_name likely should be a global temp table (ex: ##table_name)
    """
    # DEV NOTES
    # 'Consider using "df_sqlInsert_string.create_CTE_injectable_str" instead as its more sql performant
    # -- this does require both the queries to be merged to be ran'
    # 'See: NCOA_Reports.email_validations for an example.' ))
    if df.empty:
        return 'DataFrame provided to "df_sqlInsert_string" may not be empty'
    df = df.replace({np.nan: 'NULL', 'None': 'NULL', None: 'NULL'})
    df = df.replace("\'", "\'\'", regex=True)  # accounts for apostraphe's in text fields. EX: "O'Neill"
    sql_str = ''
    if table_name.startswith('#'):
        sql_str += f'DROP TABLE IF EXISTS {table_name}; \n CREATE TABLE {table_name} (\n'
        """ Column w/ data type creation """
        i = 0
        for col in df.dtypes.astype(str).items():
            if i > 0:
                sql_str += f', \n'
            c, d = col[0], pd_sql_dtype_map[col[1]]
            sql_str += f' [{c}] {d} NULL'  # Allows values to be NULL on this field in table.
            i += 1
        sql_str += '); \n'
    """ Row inserts """
    sql_str += f'INSERT INTO {table_name} ({",".join([str(x) for x in df.columns])}) \nOUTPUT INSERTED.* \nSELECT * FROM (VALUES \n '
    i = 0
    len = df.shape[1]
    for row in df.values:
        if i > 0:
            sql_str += f', \n'
        sql_str += str(tuple(row)[0:len])
        i += 1
    cols_str = str(tuple(df.columns[0:len])).replace("'", "")
    sql_str += f'\n ) AS SUB {cols_str} \n '
    sql_str += sql_insert_where + '\n'
    """ resolving NULL / NaN / None Values """
    sql_str = sql_str.replace("'NULL'", "NULL").replace(",\'\'", ",NULL")
    sql_str = sql_str.replace(',)', ')').replace('"', "'")
    return sql_str


def create_CTE_injectable_str(df:pd.DataFrame, CTE_name: str, sql_insert_where: str = None) -> str:
    """
    provides a sql script for the provided dataframe as a SQL CTE.
    This string will not work alone as it must be appended to the associated Query.
    :param: CTE_name: name for the CTE Table you're creating, Example: #TempEmailValidationTable
    """
    if df.empty:
        return 'DataFrame provided to "df_sqlInsert_string" may not be empty'
    df = df.replace({np.nan: 'NULL', 'None': 'NULL', None: 'NULL'})
    df = df.replace("\'", "\'\'", regex=True)  # accounts for apostrophe's in text fields. EX: "O'Neill"
    sql_str = f'WITH {CTE_name} AS ( \nSELECT '

    """ Column w/ data type creation """
    i = 0
    cols_str = ''
    for col in df.dtypes.astype(str).items():
        if i > 0:
            cols_str += f', '
        c, d = col[0], pd_sql_dtype_map[col[1]]
        c = str(c).replace(" ", "")
        cols_str += f'[{c}]'  # Allows values to be NULL on this field in table.
        i += 1
    sql_str += cols_str
    sql_str += '\nFROM (\n\t VALUES\n'

    """ Row inserts """
    i = 0
    len = df.shape[1]
    for row in df.values:
        if i > 0:
            sql_str += f', \n'
        sql_str += str(tuple(row)[0:len])
        i += 1
    cols_str = str(tuple(df.columns[0:len])).replace("'", "")
    sql_str += f'\n ) AS SUB {cols_str} \n '
    if sql_insert_where:
        sql_str += f' WHERE {sql_insert_where}\n'
    df_index_name = df.columns.tolist()[0]
    sql_str += f'\n)'
    """ resolving NULL / NaN / None Values """
    sql_str = sql_str.replace("'NULL'", "NULL")
    sql_str = sql_str.replace(',)', ')').replace('"', "'")
    return sql_str


def merge_df_into_sql(df:pd.DataFrame, table_name: str, sql_primary_keys_list: str|list) -> str:
    """
    provides a sql script for the provided dataframe to be merged into the tableName provided.
    using the primary_key, merging will update any existing records to match the provided values, and insert any that do not exist
    :param: df = Pandas DataFrame of the data you wish to insert into the table_name provided. 
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        !!!!    ONLY INCLUDE COLUMNS NECESSARY FOR INSERTION  !!!!!
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!        
    :param: table_name = table the data should be inserted into. should contain Schema and table name
    """ 

    if df.empty:
        ValueError("pandas DataFrame cannot be Empty")
        return 'DataFrame provided to "df_sqlInsert_string" may not be empty'
    
    if table_name.find('.') == -1:
        raise ValueError(f"value {table_name} does not include the Scheme")
    
    if type(sql_primary_keys_list) == 'str':
        sql_primary_keys_list = [sql_primary_keys_list]

    df_col_names_list = df.columns.tolist()
    for p_key in sql_primary_keys_list:
        if p_key not in df_col_names_list:
            raise ValueError(f"primary key {p_key} is not contained within the DataFrame provided")

    df = df.replace({np.nan: 'NULL', 'None': 'NULL', None: 'NULL'})
    df = df.replace("\'", "\'\'", regex=True) # accounts for apostraphe's in text fields. EX: "O'Neill"
    """ Row inserts """
    sql_df = run(df, '#ImpactList')
    sql_str = 'SET NOCOUNT ON;\n\n'
    sql_str += sql_df            
    sql_str += f'\nMERGE {table_name}\nUSING #ImpactList\n\tON '
    
    i = 0
    for prim_key in sql_primary_keys_list:
        if i > 0:
            sql_str += f'\n\tAND '
        sql_str += f'{table_name}.{prim_key} = #ImpactList.{prim_key}'
        i += 1
    
    sql_str += f'\n\tWHEN MATCHED THEN\n\tUPDATE\n\tSET\n'

    """ When the unique Identifier exists update the existing row """
    i = 0
    for col in df_col_names_list:
        if col != sql_primary_keys_list:
            if i > 0:
                sql_str += f',\n'
            sql_str += f'\t\t{table_name}.{col} = #ImpactList.{col}'
            i += 1

    sql_str += '\nWHEN NOT MATCHED THEN\n\tINSERT ('
    i = 0
    for col in df_col_names_list:
        if i > 0:
            sql_str += f', '
        sql_str += str(col)
        i += 1

    sql_str += f')\n\tVALUES\n\t\t( \n\t\t'
    i = 0
    for col in df_col_names_list:
        if i > 0:
            sql_str += f',\n\t\t'
        sql_str += f'#ImpactList.{col}'
        i += 1

    sql_str += f'\n\t\t)\nOUTPUT INSERTED.*\n;'

    """ resolving NULL / NaN / None Values """
    sql_str = sql_str.replace("'NULL'", "NULL").replace(",\'\'", ",NULL")
    sql_str = sql_str.replace(',)', ')').replace('"', "'")
    return sql_str


if __name__ == "__main__":
    file_loc = ''
    df = pd.read_csv(file_loc)
    table_name = 'NCOA.Records'
    p_key_list = ['recordId', 'NCOAFileId']

    # script = run(df)
    # script = create_CTE_injectable_str(df, CTE_name='#TempEmailValidationTable')
    sql_executable_string = merge_df_into_sql(df, table_name, p_key_list)
