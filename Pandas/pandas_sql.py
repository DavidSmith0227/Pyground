import pandas as pd
import logging
import configFolder.config as configFolder
import pyodbc
from icecream import ic
import numpy as np
import pandasql as psql
from pandasql import sqldf


def psql_insert(df:pd.DataFrame):
    sdf = psql.df


def series_toSql_format(series: pd.Series):
    """
    """
    pd_row = series.to_csv(path_or_buf=None, sep=",", na_rep='', index=False, quotechar="'")
    sql_row = f'({pd_row})'
    return sql_row


def to_sql_str_injection(df: pd.DataFrame, table_name: str):
    """

    """
    header_str = str(df.columns.to_list()).replace("[", '')
    row_adds = []
    for x in df.iterrows():
        series = x[1]

        pd_row = series.to_csv(path_or_buf=None, sep=",", na_rep='', header=False, index=False)
        sql_row = f'({pd_row})'
        row_adds.append(sql_row)
    row_adds = str(row_adds).replace("[", '').replace("]", '')

    # row_adds.append(series_toSql_format(df.apply(lambda row: row[:], axis=1)))  ## WORKING-ish
    # row_adds.append(ic([df[df.iloc[[index]]] for index in df.values()))
    sql_str = f"""
    INSERT INTO {table_name} ({header_str})
    OUTPUT INSERTED.*
    SELECT *
        FROM
            (
            VALUES
                ( {row_adds} )
            ) AS Temp(header_str)
    """
    return sql_str


if __name__ == '__main__':
    file_loc = f'//secureshare/Encrypted Share/Print Communications/Team_Members/David/SQL/Print Fulfillment/DB DDL/campaign_config/client_ready.csv'
    df = pd.read_csv(file_loc)
    to_sql_str_injection(df, table_name='MailConfiguration.ConfigStaging')


    # json_str = df.to_json(orient='table', index=False)
    # df.apply(lambda x: set_eco_value(x['Subsidy Insert']), axis=1)

    # json_str = ''
    # for index in df:
    #     row_json = df[df[index]].tojson(orient='records')
    #     json_str.append(row_json)

    # json_str = [df[index].tojson(orient='records') for index in df]
    pass