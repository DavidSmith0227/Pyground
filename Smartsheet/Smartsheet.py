import os
import csv
import time
import logging
from matplotlib.pylab import record
import pandas as pd
from icecream import ic
import configFolder.config as config
from repo.ExtendLogging import configure_logging
from dateutil.parser import parse
import Core.Data_Manager as DataManager
import Core.file_manager as FileManager
import smartsheet
from smartsheet.exceptions import ApiError

# TODO: move these to Data Manager
import repo.return_values_standard_file as rvsf
import repo.return_asset_summary as ras


#TODO: move this block to ExtendLogging
class _DunderProbeFilter(logging.Filter): 
    # Note:  Drops SDK ImportError spam caused by debugger dunder attributes (e.g. __iter__, __len__) on lazy-loading objects
    # 	"ImportError! Could not load api or model class __iter__"    
    def filter(self, record):        
        msg = record.getMessage()
        return not ("Could not load api or model class __" in msg) 
logging.getLogger('smartsheet.smartsheet').addFilter(_DunderProbeFilter())


conn = smartsheet.Smartsheet(config.smartsheet['accessToken'])
conn.errors_as_exceptions(True)


def is_sheet(value) -> bool:
    if isinstance(value, smartsheet.models.Sheet):
        return True
    else:
        return False


def return_smartsheet_connection():
     return conn 


def model_row():
    row_model = conn.models.Row()
    return row_model


def model_cell():
    cell_model = conn.models.Cell()
    return cell_model

# ---------------------------
# Sheet | DF
# ---------------------------
def get_sheetid(sheet_name: str):
	"""
	Finds the sheet_id via sheet_name
	"""
	all_sheets = conn.Sheets.list_sheets(include_all=True)
	for sheet in all_sheets.data:
		if sheet.name == sheet_name:
			print(f'{sheet_name}: {sheet.id}') # for Debugging
			return sheet.id
	print(f'No Sheet with name: "{sheet_name}" was found. Check the name and try again.')


def get_sheet(sheet_request):
    """
    Universal command to pull the SmartSheet Sheet from known available sources.
    ie: sheet_id, sheet_name, or the Sheet itself
    """
    if is_sheet(sheet_request):
        return sheet_request
    elif not DataManager.is_integer(sheet_request):
        sheet_id = get_sheetid(sheet_request)
    else:
         sheet_id = sheet_request 
    sheet = conn.Sheets.get_sheet(sheet_id)
    return sheet

	
def return_all_sheet_ids():
    all_sheet_ids = []
    ss_id_config = config.shared_directory['smartsheet_backup_list']
    with open(ss_id_config, 'r')as obj_file:
        reader = csv.reader(obj_file)
        next(reader)
        for row in reader:
            logging.info(row[0] + ' request to backup.')
            sheet_id = get_sheetid(row[0])
            all_sheet_ids.append(sheet_id)
    return all_sheet_ids


def smartsheet_to_dataframe(sheet_id: int) -> pd:
	"""
	:param sheet_id: can be int or string
	:return: dataframe 
        # NOTE: this also provides the ss_row_id
	"""
	sheet = get_sheet(sheet_id)
	col_names = [col.title for col in sheet.columns]
	col_names.append("ss_row_id")
	rows = []
	for row in sheet.rows:
		cells = []
		for cell in row.cells:
			cells.append(cell.value)
		cells.append(row.id)
		rows.append(cells)
	data_frame = pd.DataFrame(rows, columns=col_names)
	return data_frame


def return_child_rows(sheet_id: int, parent_row_id: int):
    token = config.smartsheet['accessToken']
    ss = smartsheet.Smartsheet(token)
    sheet = ss.Sheets.get_sheet(sheet_id)

    col_names = [col.title for col in sheet.columns]
    col_names.append("ss_row_id")
    rows = []
    for row in sheet.rows:
        cells = []
        if row.parent_id == parent_row_id:
            for cell in row.cells:
                cells.append(cell.value)
            cells.append(row.id)
            rows.append(cells)
    data_frame = pd.DataFrame(rows, columns=col_names)
    return data_frame


def export_smartsheet_to_csv(sheet_id, output_filepath):
    """
    Export a single Smartsheet to an Excel file
    
    Args:
        sheet_id (int): Smartsheet ID to export
        output_filepath (str): Full path to output Excel file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        df = smartsheet_to_dataframe(sheet_id)
        df.to_csv(output_filepath, index=False)
        return True
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return False
    

# ---------------------------
# Row
# ---------------------------
def add_row(sheet_name_or_id, add_row):
	"""
	Creates smartsheet row, if created in Via Benefits Communications Management, returns FSID
	:param sheet_name: Smartsheet name str, or can use sheetId int
	:param add_row: smartsheet_repo row
	:return:FSID optional, row id if sheet is defined as email camp attachment
	"""
	sheet = get_sheet(sheet_name_or_id)
	sheet_id = sheet.id
	row = smartsheet.models.Row()
	row.toBottom = True
	column_map = {}

	for column in sheet.columns:
		column_map[column.title] = column.id
		for new_value in add_row:
			new_value_split = new_value.split(':')
			if new_value_split[0] == column.title and str(column.type) == 'CHECKBOX':
				true_values = ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 1, True, 'True']
				if new_value_split[1] in true_values:
					bool_value = True
				else:
					bool_value = False
				row.cells.append({
					'column_id': column_map[column.title],
					'value': bool_value
				})
			elif new_value_split[0] == column.title:
				row.cells.append({
					'column_id': column_map[column.title],
					'value': new_value_split[1]
				})
	response = conn.Sheets.add_rows(sheet_id, [row])
	row_id = response.data[0].id_
	if sheet_id == int(config.smartsheet['communication_sheet_id']): # 'Via Benefits Communications Management'
		fsid = response.data[0].cells._TypedList__store[5].value
		return fsid, row_id
	else:
		return row_id


def update_rows(sheet_id: int, rows: list):
	return conn.Sheets.update_rows(sheet_id, rows)


def update_row_by_column(sheet_id, row_ids_list: list, column_name, update_value):
    column_id = get_column_id(sheet_id, column_name)
    row_updates = []
    for row_id in row_ids_list:
        row_updates.append({"cells": [{"columnId": column_id, "value": update_value}], "id": row_id})
    results = conn.update_rows(sheet_id, [row_updates])  # , allowPartialSuccess=True
    return results


def update_row_by_column_value(sheet_id: int, row_id: int, col_name: str, new_value) -> str: 
    """
    Updates smartsheet row
    :param sheet_id: sheet id, can be int or string
    :param row_id: Must be int
    :param col_name: Name of column to update
    :param new_value: New value
    :return: Status of update; SUCCESS or ERROR
    """
    column_map = {}
    sheet = conn.Sheets.get_sheet(sheet_id)
    for column in sheet.columns:
        column_map[column.title] = column.id
    if col_name in column_map:
        newCell = conn.models.Cell()
        newCell.column_id = column_map[col_name]
        newCell.value = new_value

        newRow = conn.models.Row()
        newRow.id = row_id
        newRow.cells.append(newCell)
        try:
            response = conn.Sheets.update_rows(sheet_id, [newRow])
            update_status = response.message
        except Exception as e:
            logging.error(ic(e))
            update_status = 'ERROR'
    else:
        logging.error(ic(f"{col_name} does not exist in sheet: {sheet_id} \n@URL: {sheet.permalink}"))
        update_status = 'ERROR'
    return update_status


def get_rowids(sheet_id: str) -> list:
    """

    :param ss: smartsheet object
    :param sheet_id:
    :return: list of all row ids
    """
    row_id_list = []
    sheet = get_sheet(sheet_id)
    for row in sheet.rows:
        row_id_list.append(row.id)
    return row_id_list


def return_row_id_from_matching_values(sheet_id: int, cell_values: list):
    """ Generic search for any matching value within the entire sheet"""
    sheet = get_sheet(sheet_id)
    row_details = []
    for value in cell_values:
        for row in sheet.rows:
            for cell in row.cells:
                if cell.value == value:
                    rowid = row.id
                    row_details.append(rowid)
    return row_details


def get_row_id_from_matching_column_values(sheet_id, matching_col_name, matching_col_value):
    sheet = get_sheet(sheet_id)
    map = get_column_map(sheet)
    col_id = map[matching_col_name]
    rows = sheet.rows
    row_id = None
    for row in rows:
        for cell in row.cells:
            if cell.column_id == col_id:
                if cell.display_value == matching_col_value:
                    row_id = row.id
    return row_id


def delete_rows(sheet_id: str, row_ids: list) -> str:
    """
    Deletes smartsheet rows, typically you process multiple row_ids, however at the time it could not handle 685.
    So a loop was put in place.
    :param sheet_id:
    :param row_ids: list of ints
    :return: Result message
    """
    status = 'SUCCESS'
    for row_id in row_ids:
        result = conn.Sheets.delete_rows(sheet_id, row_id)
        ic(f"Row {row_id} removed from sheet # {sheet_id}")
        if result.message != 'SUCCESS':
            status = 'ERROR'
    return status


def return_smartsheet_last_created_record_id(sheet_name, created_column_name):
	"""
	param: sheet_name May be sheet_id or Sheet Name
	param: created_column_name
	"""
    
	sheet_id = get_sheetid(sheet_name)
	sheet = get_sheet(sheet_id)
    
	column_map = {}
    # Build column map for later reference - translates column names to column id
	for column in sheet.columns:
		column_map[column.title] = column.id
	latest_date = ''
	for row in sheet.rows:
		created_date = parse(get_cell_by_column_name(sheet_id, row, created_column_name).value)
		if created_date:
			latest_date = created_date
		elif created_date > latest_date:
			latest_date = created_date
	return row.id


def sort_ss_rows(sheet_name_or_id, sort_by_col_name):
    sheet = get_sheet(sheet_name_or_id)
    sheet_id = sheet.id
    column_map = {}
    for column in sheet.columns:
        column_map[column.title] = column.id
    sort_by_col_id = column_map.get(sort_by_col_name)
    sort_specifier = conn.models.SortSpecifier({
        'sort_criteria': [conn.models.SortCriterion({
            'column_id': sort_by_col_id,
            'direction': 'ASCENDING'
        })]
    })
    conn.Sheets.sort_sheet(sheet_id, sort_specifier)


# ---------------------------
# Columns
# ---------------------------
def list_columns(sheet_id: int):
	return conn.Sheets.list_columns(sheet_id)


def add_column(sheet_id: int, column_spec):
	return conn.Sheets.add_columns(sheet_id, [column_spec])


def get_column_name(sheet_id: int, col_id: int):
    sheet = get_sheet(sheet_id)
    column_Name = 'not found'
    for col in sheet.columns:
        if col.id == col_id:
            column_Name = col.title
    return column_Name


def get_column_id(sheet_id: int, column_name_match: str):
	sheet = get_sheet(sheet_id)
	for column in sheet.columns:
		if str(column.title).lower() == column_name_match.lower():
			col_id = column.id
			return col_id
		else:
			continue


def return_column_info(sheet_id, column):
    """
    :param: sheet: int|str - sheet_id or sheet_name
    :param: column: int/str - column_id or column_name
    """
    if type(column) == int:
        column_id = column
    else:
        response = conn.Sheets.get_columns(sheet_id)
        for column_data in response.data:
            ID = column_data.id
            title = column_data.title
            if title.lower() == column.lower():
                column_id = ID
                print(f'{column_id}: {title}')
                break
    column_details = conn.Sheets.get_column(sheet_id, column_id)
    return column_details


def map_column_name_to_ids(sheet_name_or_id, column_names):
    #TODO: add fuzzy search
    column_ids_to_return = []
    sheet = get_sheet(sheet_name_or_id)
    for column in sheet.columns:
        if column.title in column_names:
            column_ids_to_return.append(column.id)
    if len(column_names) == len(column_ids_to_return):
        return column_ids_to_return
    else:
        raise Exception(f'{len(column_names)} column names were submitted, '
                        f'but only {len(column_ids_to_return)} were found.')


def get_column_map(sheet) -> dict:
    """
    Generates a map between column names and their corresponding column IDs
    @param sheet_id: Sheet ID for the smartsheet
    @return: Dictionary of column name, column ID pairs
    """
    if not is_sheet(sheet):
        sheet = get_sheet(sheet)
    column_map = {}
    for column in sheet.columns:
        column_map[column.title] = column.id
    return column_map


# ---------------------------
# Cells
# ---------------------------
def update_cell(sheet_id: int, row_id: int, column_id: int, value):
	"""
	Updates a single cell by constructing a row update payload.
	"""
	row = conn.models.Row()
	row.id = row_id

	cell = conn.models.Cell()
	cell.column_id = column_id
	cell.value = value

	row.cells = [cell]

	return conn.Sheets.update_rows(sheet_id, [row])


def update_cell_value(row_ids: list, sheet_id: int, col_id: int, new_value):
    """
    updates a field in multiple rows in the given column
    :param row_ids: must be ints
    :param sheet_id:
    :param col_id:
    :param new_value: This type must match the data type in smartsheet
    :return: returns response
    """
    #TODO: add col_name functionality to col_id for better user experience
    sheet = get_sheet(sheet_id)
    all_row_updates = []
    for ss_row in sheet.rows:
        cell_update = conn.models.Row()
        cell_update.id = ss_row.id
        cell_update.column_id = col_id
        if ss_row.id in row_ids:
            adjusted_cell = conn.models.Cell()
            adjusted_cell.column_id = col_id
            adjusted_cell.value = new_value
            cell_update.cells.append(adjusted_cell)
            all_row_updates.append(cell_update)
    if all_row_updates:
        updated_row = conn.Sheets.update_rows(sheet.id, all_row_updates)
        result = updated_row.message
        ic(result)
        

def get_cell_by_column_name(sheet, row, column_name):
    column_map = get_column_map(sheet)
    column_id = column_map[column_name]
    return_value = row.get_column(column_id).value
    # prior used value for Not-Date values -- return_value = row.get_column(column_id).display_value
    return return_value


# ---------------------------
# FSID
# ---------------------------
def send_fsid_to_sun(file_name: str, message: str = 'Here\'s an FSID for your records', row_id: int = ''):
    """
    Sends FSID to Sun, by default sends the last one
    :param file_name: File basename
    :param message: Message to accompany the sent record
    :param row_id: smartsheet row id
    :return:
    """

    smartsheet_sheet_name = config.smartsheet['communication_sheet_name']
    """Getting last record added to smartsheet_repo and sending via ss email."""
    created_column_name = 'DateSubmitted'

    if row_id:
        smartsheet_row_id = row_id
    else:
        smartsheet_row_id = return_smartsheet_last_created_record_id(smartsheet_sheet_name, created_column_name)
    """Set up email"""
    column_names_to_include = config.smartsheet['jobs_to_vendor_columns']
    sheet = get_sheet(smartsheet_sheet_name)
    column_map = get_column_map(sheet)

    filtered_column_map = {k: v for k, v in column_map.items() if k in column_names_to_include}
    column_ids_to_include = list(filtered_column_map.values())
    po_recipient = config.email['sun_po']
    subject = f'{file_name}'
    msg = message
    email_result = email_smartsheet_record(smartsheet_sheet_name, column_ids_to_include, smartsheet_row_id, po_recipient, msg, subject)
    logging.info(f'Smartsheet PO sent {file_name}. Response {email_result}')
    

def email_smartsheet_record(sheet_name, colum_ids_to_include, row_id, email_recipient, msg, subject):
    stripped_email = email_recipient.replace(';','')
    sheet_id = get_sheetid(sheet_name)

    """Configure email"""
    email = conn.models.MultiRowEmail()
    email.send_to = conn.models.Recipient({'email': stripped_email})
    email.include_attachments = False
    email.include_discussions = False
    email.row_ids = [row_id]
    email.column_ids = colum_ids_to_include
    email.subject = subject
    email.message = msg
    email.include_discussions = True
    email.cc_me = True
    response = conn.Sheets.send_rows(sheet_id, email)
    return response


def convert_fsid_obj_to_str(FSID):
    fsid_list = []
    for key, value in FSID.__dict__.items():
        if str(key) != 'all_warnings' and value is not None:
            col_value = f"{key}:{value}"
            fsid_list.append(col_value)

    return fsid_list


def create_fsid(file_path: str, mailing_type: str):
    """
    Takes input of various values and creates an FSID on the communication smartsheet
        :file_path: str, used for Description field
        :mailing_type: str, config name in fsid_config (if mailchimp, should be the same mailing_type throughout)
    """
    # TODO: update with Sendgrid details when we begin using them

    # TODO: Use File Manager
    df = pd.read_csv(file_path)

    '''pad drop date with leading zero in print file'''
    drop_date = df['dropDate'].iloc[0]
    asset_dict = ras.return_asset_breakdown(df)
    file_base_name = os.path.basename(file_path)

    mail_config = config.fsid[mailing_type]
    vendor = mail_config['vendor']
    description = mail_config['description'] if mail_config.get('description') and mail_config['description'] != '' else f"{file_base_name.split('.')[0]}"

    config_comments = mail_config.get('additional_comments') if mail_config.get('additional_comments') is not None else ''

    new_fsid_row_dict = {
            'assigned_to': 'pennyworthproject@darkknight.com',
            'Description':  description,
            'Vendor': mail_config['vendor'],
            'CommunicationType': mail_config['communication_type'], # Required: Used for financial reporting        
            'DataFileName': file_base_name,
            'Quantity': str(asset_dict['total']),
            'Target_DropDate': DataManager.reformat_date(drop_date, '%Y-%m-%d'), # Format: MM/DD/YYYY
            'po_submitted': DataManager.get_today('%Y-%m-%d'), # Format: MM/DD/YYYY
            'asset_pstd': 'true', 
            'data_pstd': 'true',
            'submitted_to_vendor': 'true',
            'additionalComments': config_comments + ''.join(f'{k}, qty = {v}\n' for k, v in asset_dict.items()),
            'AssetName': ', '.join(k for k in asset_dict if k != 'total')  
            }
    
    if 'sun' in str(vendor).lower():
        new_fsid_row_dict.update({
            'comm_type': 'print',
            'vendor': 'Sun Print Solutions',
            'proof': 'SFTP',
            'data_file_location': '/ENCRYPTED_FILES/',
            'asset_location': 'SFTP',
            'VariablePrint': mail_config['variable_print'], # Yes | No
            'MailOnHouseholdLevel': mail_config['householded'], # Yes | No
            'Trim (flat size)': mail_config['trim'], 
            'Finish (final size)': mail_config['finish'], 
            'Color': mail_config['color'],  # 1 = Black & White, 4 = Color, 0 = blank. x/y x=front of page, y=back of page. EX: 4/4
            'Paper': mail_config['paper'], # text | card
            'postage': mail_config['postage'] if mail_config.get('postage') and mail_config['postage'] != '' else 'First Class',
            'EnvelopeSize': mail_config['envelope'] if mail_config.get('postage') else '', # may be blank for Postcards
            'permit': mail_config['permit'] if mail_config.get('permit') and mail_config['permit'] != '' else 'Permit 766 (Via Benefits)',   
            'is_email_campaign': 'False' # str for smartsheet intake
        })

    elif 'chimp' in str(vendor).lower():
        new_fsid_row_dict.update({
            'comm_type': 'email',
            'vendor': 'MailChimp',
            'proof': 'Other',
            'data_file_location': '/Archive/',
            'asset_location': 'Mailchimp',
            'is_email_campaign': 'True', # str for smartsheet intake
        })
        
    # elif 'sendgrid' in str(vendor).lower():
    #     raise ValueError(f'We dont use SendGrid yet, silly!')
    
    sheet_id = config.smartsheet['communication_sheet_name'] # Via Benefits Communications Management
    # TODO: something is really slow on add_row
    row_id = add_row(sheet_id, new_fsid_row_dict)
    return row_id


# ---------------------------
# Custom // Other
# ---------------------------
def convert_df_to_smartsheet_importable(df: pd.DataFrame):
    """
    Converts columns and rows from a Pandas DataFrame into a SmartSheet importable Layout IE

    ('column:rowValue', 'column:rowValue'......)
    :rtype: pd.DataFrame
    """
    smartsheet_all = []
    for index, row in df.iterrows():
        smartsheet_str = []
        for ind, column in enumerate(df.columns):
            # print(column, row.get_value(column))
            smartsheet_val = str(column) + ':' + str(row.at[column])
            smartsheet_str.append(smartsheet_val)
        smartsheet_all.append(smartsheet_str)
    return smartsheet_all   


def df_to_ss_with_row_id(sheet_id, df):
    """
    Function that updates a smartsheet from a dataframe. IMPORTANT NOTE: This function assumes that
    the dataframe contains a rowID column (i.e. the rowIDs of the rows that need to be updated are
    known)
    @param sheet_id: SheetID of the smartsheet to which the columns will be updated
    @param df: dataFrame of rows to update the smartsheet
    @return:
    """
    sheet = get_sheet(sheet_id)
    col_name_list = list(df.columns)
    col_name_id_dict = get_column_map(sheet)
    all_rows_list = []

    if 'ss_row_id' in col_name_list:
        col_name_list.remove('ss_row_id')
    for index, row in df.iterrows():
        new_row = conn.models.Row()
        new_row.id = row['ss_row_id']
        for i in range(len(col_name_list)):
            col_name = col_name_list[i]
            if col_name in col_name_id_dict.keys():
                col_id = col_name_id_dict[col_name]
                new_cell = conn.models.Cell()
                new_cell.column_id = col_id
                new_cell.value = row[col_name]
                file_new_value = new_cell
                new_row.cells.append(file_new_value)
        all_rows_list.append(new_row)
    try:
        conn.Sheets.update_rows(sheet_id, all_rows_list)
    except Exception as e:
        print("Error occured while attempting to update smartsheet. Check that the correct sheet_id is used.")
        print("EXCEPTION:", e)
 

def add_attachment_to_row(row_id: int, sheet_id: int, file_path: str) -> dict:
    """
    Adds attchment to smartsheet_repo rows
    :param row_id: int row id
    :param sheet_id: int sheet id
    :param file_path: file and path
    :return: response
    """
    base_name = os.path.basename(file_path)

    with open(file_path, 'rb') as file:
        updated_attachment = conn.Attachments.attach_file_to_row(
            sheet_id,
            row_id,
            (base_name, file, 'application/csv')
        )
    return updated_attachment


def add_to_smartsheet_reporting(reporting_file_path, sheet_id):
    import Core.Data_Manager as data
    ss_row = [f'file_name:{os.path.basename(reporting_file_path)}', f'date:{data.get_today(date_format = "%Y-%m-%d")}']
    row_id = add_row(sheet_id, ss_row)
    add_attachment_to_row(row_id, sheet_id, reporting_file_path)
    return


def create_error_record_str(script_name, error_msg, file_name='', file_location=''): # Prior name: "create_error_record"
    """
    :param script_name:
    :param error_msg:
    :param file_name:
    :param file_location:
    :return:
    """
    smartsheet_record = [f'Script Name:{script_name}', f'File Name:{file_name}',
                         f'New File Location:{file_location}',
                         f'Error Message: {error_msg}']
    return smartsheet_record


def backup_smartsheet():
    logging.info('STARTED:\n\tsmartsheet_api_defs/back_up.py.backup_smartsheet()')
    # shared_drive_letter = config.directory['print_comm_drive_letter']

    backup_archive = config.shared_directory['smartsheet_backup_archive']
    sheet_ids = return_all_sheet_ids()
    for ID in sheet_ids:
        time.sleep(2)
        conn.Sheets.get_sheet_as_excel(ID, backup_archive, alternate_file_name=None)
        logging.info(f'Sheet id {ID} archived to {backup_archive}')
        print(f'Sheet id {ID} archived to {backup_archive}')
    logging.info('COMPLETED:\n\tsmartsheet_api_defs/back_up.py.backup_smartsheet()')


if __name__ == '__main__':
    pass
    file_base_name = 'TEST_FSID_CREATION'
    drop_date = '20900727'
    asset_dict = {'Test_asset': 3, 'total': 3}
    vendor = 'Sun Print Solutions'
    vendor = 'Sun'
    # vendor = 'Mailchimp'
    create_fsid(file_base_name, drop_date, asset_dict, vendor, send_fsid=False)