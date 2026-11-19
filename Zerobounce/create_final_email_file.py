import os
import logging
import pandas as pd
import configFolder.config as config
import repo.file_encode_detect as fed


def return_scrubbed_email_file(zb_results_file: str, email_file: str) -> str:
    """
    Returns email file with 'valid' and 'catch-all' ZB results
    :param zb_results_file: Zero Bounce results file
    :param email_file: email file and path, used for file name
    :return: out_file_name_path: scrubbed file name, written to same directory
    """
    status = f"create_final_email_file.py.return_scrubbed_email_file() started."
    logging.info(status)
    tempDirectory = config.shared_directory['temp_dir']
    file_basename = os.path.basename(email_file).split(".")[0]
    out_file_name_path = os.path.join(tempDirectory, f"{file_basename}_zb_scrubbed.csv")
    file_encode = fed.return_file_encoding(zb_results_file)
    encoded_clients = {'Corteva Agriscienceâ„¢': 'Corteva Agriscience™', 'Corteva Agriscience\x99':'Corteva Agriscience™','NestlÃ©': 'Nestlé'}
    
    try:
        df = pd.read_csv(zb_results_file, encoding=file_encode)
    except UnicodeDecodeError:
        df = pd.read_csv(zb_results_file, encoding='cp1252')  # fallback


    e_df = pd.read_csv(email_file, encoding=fed.return_file_encoding(email_file))
    df.replace(encoded_clients, inplace=True)
    email_df = df.copy()
    email_df = email_df.loc[(df['ZB Status'] == 'valid') | (df['ZB Status'] == 'catch-all')]
    email_df.drop(email_df.columns.difference(e_df.columns), inplace=True, axis=1)
    email_df.to_csv(out_file_name_path, index=False, encoding=file_encode, lineterminator='\r\n')
    status = f"{out_file_name_path} generated."
    logging.info(status)
    return out_file_name_path


if __name__ == "__main__":
    pass
