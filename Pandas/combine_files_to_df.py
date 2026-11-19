import pandas as pd


def combine_data_files(working_dir: str, found_files: list) -> pd:
    """
    Combines files to data frame, must be the same header format
    :param working_dir: directory where files are located
    :param found_files: list of files to combine into data frame
    :return: df
    """
    df_list = []
    for file in found_files:
        df = pd.read_csv(working_dir + file, header=0, encoding='cp1252')
        df['data_file_name'] = file
        df_list.append(df)
    return pd.concat(df_list, sort=False)