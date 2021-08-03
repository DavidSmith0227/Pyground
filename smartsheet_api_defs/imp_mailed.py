import smartsheet
import configFolder.config as config


def return_fsid_list(sheet, po_id, status_id):
    mailed_list = []
    for row in sheet.rows:
        status = row.get_column(status_id).display_value
        po_number = row.get_column(po_id).display_value
        if status == 'Mailed':
            mailed_list.append(po_number)
    return mailed_list


def return_imps_dict(sheet, po_id, imp_col_id):
    result_dict = {}
    for row in sheet.rows:
        po_number = row.get_column(po_id).display_value
        imp_id = row.get_column(imp_col_id).display_value
        if po_number:
            result_dict[po_number] = imp_id
    return result_dict


def return_ss_sheet (sheet_id, columns):
    token = config.smartsheet['accessToken']
    ss = smartsheet.Smartsheet(token)
    sheet = ss.Sheets.get_sheet(sheet_id, column_ids = columns)
    return sheet


def run():
    try:
        mailed_imp_ids = []
        fsid_sheet_id = '5704624016516996'
        fsid_po_col_id = 1235583008827268
        fsid_status_col_id = 8087338564773764
        fisd_columns = [fsid_po_col_id, fsid_status_col_id]
        fsid_sheet = return_ss_sheet(fsid_sheet_id, fisd_columns)
        fsid_mailed = return_fsid_list(fsid_sheet, fsid_po_col_id, fsid_status_col_id)

        client_imp_sheet_id = '3102331467261828'
        client_po_col_id = 6541438702905220
        client_imp_col_id = 6555152466372484
        client_imp_columns = [client_po_col_id, client_imp_col_id]
        client_imp_sheet = return_ss_sheet(client_imp_sheet_id, client_imp_columns)
        client_imp_dict = return_imps_dict(client_imp_sheet, client_po_col_id, client_imp_col_id)

        for key in client_imp_dict:
            if key in fsid_mailed:
                mailed_imp_ids.append(client_imp_dict[key])
        return mailed_imp_ids
    except Exception as e:
        print(f'The following error has occurred: {str(e)}')
        return None

if __name__ =='__main__':
    mailed_client_imps = run()
    print(mailed_client_imps)
