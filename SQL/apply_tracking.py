import os
import sys
import logging

from numpy.f2py.auxfuncs import throw_error
from paramiko.agent import value

import configFolder.config as config
import pandas as pd
import sql_functions.insert_asset as insert_asset
import sql_functions.df_sqlInsert_string as sql_str
import repo.queries as sql
import participant_communication.ppt_repo.ColumnNameMappingRepo as col_mapping
import shutil
import repo.detect_delimiter as get_delimiter
import repo.file_encode_detect as fed


def apply_communications_tracking(file_path, allow_sequencing: bool = False):
    delimiter = get_delimiter.detect_delimiter(file_path)
    file_base_name = os.path.basename(file_path)
    df = pd.read_csv(file_path, delimiter=delimiter, encoding=fed.return_file_encoding(file_path))
    df = col_mapping.standardize_df_column_names(df)
    assets_list = df['assetname'].drop_duplicates(keep='first').dropna().to_list()
    assets_list = [i for i in assets_list if i.lower() != 'null']
    if len(assets_list) == 0 or df.shape[0] == 0:
        err_msg = f'no records/assets on {file_base_name}: please review'
        logging.error(err_msg)
        raise ValueError(err_msg)
    if len(assets_list) >= 10:
        err_msg = f'Critical Warning on asset insert: too many assets found. please review {file_base_name}.'
        logging.error(err_msg)
        raise ValueError(err_msg)
    for asset in assets_list:
        template_id = insert_asset.run(asset_name=asset)
        df = df.replace([asset], f'{template_id}')

    """ creating global temp table """
    df_insert = df[['customerPersonId', 'campaignId', 'dropDate', 'assetname']]
    df_insert.rename(columns={'assetname': 'templateId', 'customerPersonId': 'personId', 'dropDate': 'receiveDate'}, inplace=True)
    server, database, table = 'ProdSqlAg', 'ProdCommunicationsTracking', '##TrackingInsert'

    """ pulling a str of sql tracking statement """
    sql_template = open(os.path.join(config.shared_directory['sql_auto'],'Automation/Tracking/InsertIntoCommsTracking_forHotFolder.sql'), 'r')
    sql_insert_str = sql_template.read()
    sql_insert_str = sql_insert_str.replace('@@allowSequencing', str(f"'{allow_sequencing}'"))

    conn = sql.sql_connect(server=server, database=database)
    cursor = conn.cursor()
    insert_amnt = df_insert.shape[0]
    batch_size = 1000
    inserted_total = 0
    logging.info(
        f'Applying communications tracking to {file_base_name}: '
        f'{insert_amnt} rows in batches of {batch_size}'
    )
    try:
        for start in range(0, insert_amnt, batch_size):
            batch = df_insert.iloc[start:start + batch_size]
            batch_amnt = batch.shape[0]
            sql_temp_table_str = sql_str.run(df=batch, table_name=table)
            final_sql_str = str(sql_temp_table_str + '\n' + sql_insert_str)
            cursor.execute(final_sql_str)
            sql_return_amnt = len(cursor.fetchall())
            while cursor.nextset():
                pass
            logging.info(
                f'Tracking batch {start // batch_size + 1}: '
                f'expected {batch_amnt}, inserted {sql_return_amnt}'
            )
            if sql_return_amnt != batch_amnt:
                err_msg = (
                    f'ERROR__ Communications Tracking failed to insert all rows '
                    f'for file: "{file_base_name}". Inserted {sql_return_amnt} '
                    f'out of {batch_amnt} rows in batch starting at {start}'
                )
                logging.error(err_msg)
                raise Exception(err_msg)
            inserted_total += sql_return_amnt
        cursor.commit()
        cursor.close()
        success_msg = (
            f'Communications Tracking applied to file: {file_base_name}. '
            f'Inserted {inserted_total} of {insert_amnt} rows'
        )
        logging.info(success_msg)
        print(success_msg)
    except Exception as e:
        cursor.rollback()
        cursor.close()
        err_msg = (
            f'ERROR__ Communications Tracking Failed on file: {file_base_name}. '
            f'Inserted {inserted_total} of {insert_amnt} rows before failure. {e}'
        )
        logging.error(err_msg)
        print(err_msg)
        raise


if __name__ == "__main__":
    file_loc = config.shared_directory["communications_tracking_hot_folder"]
    file_name = 'FileNameHere'
    file_path = os.path.join(file_loc, file_name)
    apply_communications_tracking(file_path, True)
