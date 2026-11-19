import os
import logging
import pandas as pd
from icecream import ic


def run(df_list: list) -> pd.DataFrame:
    df_merge = pd.DataFrame(columns=df_list[0].columns, data=None)
    for df in df_list:
        if str(df_merge.columns).lower() == str(df.columns).lower():
            try:
                df_merge = pd.concat([df_merge, df], ignore_index=True)
            except Exception as e:
                logging.error(e)
        else:
            error = f'combine_dataFrames.py failed due to mismatching columns \n' \
                    f'df-1 columns: {df_merge.columns} \n' \
                    f'df-2 columns: {df.columns} '
            print(error)
            logging.error(error)
    return df_merge


if __name__ == '__main__':
    loc = r'\\secureshare\Encrypted Share\Print Communications\Team_Members\MOD_data\Age-In Mailings\Test Files'

    # file_1 = 'ALFRED_LockheedMartin_CertifiedNonResponderMailFile_20211216.csv'
    # file_2 = 'ALFRED_NorthrupGrumman_CertifiedNonResponderMailFile_20211202.csv'

    file_1 = 'TEST_Corteva_Certified.csv'
    file_2 = 'TEST_IBM_Certified.csv'
    file_3 = 'TEST_Lockheed_Certified.csv'
    file_4 = 'TEST_Northrop_Certified.csv'

    df_1 = pd.read_csv(os.path.join(loc, file_1), engine='python', index_col=None)
    df_2 = pd.read_csv(os.path.join(loc, file_2), engine='python', index_col=None)
    df_3 = pd.read_csv(os.path.join(loc, file_3), engine='python', index_col=None)
    df_4 = pd.read_csv(os.path.join(loc, file_4), engine='python', index_col=None)

    test_merge_df = run([df_1, df_2, df_3, df_4])
    # test_merge_df = run([df_1, df_2])

    print(test_merge_df.size)
    pass