import os
import logging
import pandas as pd
from icecream import ic
import repo.file_encode_detect as fed


def return_rejected_zb_df(file_path: str) -> pd:
    """
    Returns print ready dataframe of ZB results
    :param file_path: File and path of ZB results file
    :return: rejected_df: Data frame of rejected records, updated for print file
    """
    status = "zb_rejected_records.py.return_rejected_zb_df() started."
    logging.info(ic(status))
    file_encoding = fed.return_file_encoding(file_path)
    try:
        df = pd.read_csv(file_path, encoding=file_encoding)
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='cp1252')  # fallback for encoding issues
    rejected_record_df = df.copy()
    rejected_record_df = rejected_record_df.loc[~((rejected_record_df['ZB Status'] == 'valid')
                                                  | (rejected_record_df['ZB Status'] == 'catch-all'))]
    cols_to_drop = ['ZB Status', 'ZB Sub status', 'ZB Account', 'ZB Domain', 'ZB First Name', 'ZB Last Name',
                    'ZB Gender', 'ZB Free Email', 'ZB MX Found', 'ZB MX Record', 'ZB SMTP Provider', 'ZB Did You Mean']
    
    existing_cols_to_drop = [col for col in cols_to_drop if col in rejected_record_df.columns]
    rejected_record_df.drop(existing_cols_to_drop, axis=1, inplace=True)
    
    # rejected_df.asset1 = rejected_df.asset1.str.replace("-EMAIL", "")
    status = f"{rejected_record_df.shape[0]} records rejected from ZB."
    logging.info(ic(status))
    file_basename = os.path.basename(file_path).split(".")[0]
    out_file_name_path = f"{os.path.dirname(file_path)}/{file_basename}_zb_rejects.csv"
    rejected_record_df.to_csv(out_file_name_path, index=False)
    status = "zb_rejected_records.py.return_rejected_zb_df() completed."
    logging.info(ic(status))
    return rejected_record_df


if __name__ == "__main__":
    df = return_rejected_zb_df('U:/MAV_DropDate-20220418-MultiCampaign-20220405_email_zb_results.csv')
    ic()
    pass
