import pandas as pd
import os
import configFolder.config as config
import pyodbc
import logging
from icecream import ic
import repo.queries as sql
from thefuzz import fuzz, process
import Core.Smartsheet as ss


def make_asset_review_task(asset_name):
    sheet_id = 398478282805124  # ss_name = asset_reporting_watchlist
    new_row = [f'asset_name: {asset_name}']
    try:
        ss.add_row(sheet_id, new_row)
        ic(logging.info(f"Asset added to watchlist for review: {asset_name}"))
    except Exception as e:
        ic(logging.error(f"Asset failed to add to watchlist for review: {asset_name}\nWith ERROR: {e}"))
    return


def identify_template_typeid(asset_name):
    email_string_identifier_list = ['EML', 'EMAIL', 'E-MAIL', 'MAV', 'WEBINAR']
    if bool([ele for ele in email_string_identifier_list if(ele in asset_name.upper())]):
        comm_type_id = 7  # ad-hoc Email
    else:
        comm_type_id = 3  # ad-hoc Print
    return comm_type_id


def estimate_template_reporting_type(asset_name):
    type_matching_dict = {  # all key's as LowerCase for best matching...
        'agein': 'AgeInSuite',
        'age-in': 'AgeInSuite',
        'age_in': 'AgeInSuite',
        'guide': 'AgeInSuite',
        'u1c': 'AgeInSuite',
        'greg': 'InitialSuite',
        'intro': 'InitialSuite',
        'spd': 'HRA',
        'initialsuite': 'AgeInSuite',
        'discover': 'Discover',
        'groove': 'Groove',
        'mav': 'MAV',
        'nonresponder': 'NonResponder',
        'certified': 'NonResponder',
        'welcome': 'Welcome',
        'custom': 'Custom'
    }
    type_matching_dict = dict((k.lower(), v) for k, v in type_matching_dict.items())
    match_key_list = list(type_matching_dict.keys())
    s = process.extractOne(asset_name, match_key_list)
    if s[1] > 70:
        ic(logging.info(f'Asset Reporting Type matching compared "{asset_name}" and found {s[0]} with {s[1]}% certainty'))
        comm_reporting_type_match = type_matching_dict[s[0]]
    else:
        ic(logging.info(f'Asset Reporting Type matching compared "{asset_name}" and found NO CONFIDENT MATCHES'))
        comm_reporting_type_match = 'Needs_Review'
    return comm_reporting_type_match


def estimate_template_reporting_description(asset_name, comm_reporting_type):
    desc_matching_dict = {
        'AgeInSuite': {
            '12mo_pre': '12mo_pre',
            '12mo_Webinar': '12mo_Webinar',
            '2&3mo_post': '2&3mo_post',
            '2mo_pre': '2mo_pre',
            '3mo_pre': '3mo_pre',
            '4mo_pre': '4mo_pre',
            '6mo_pre': '6mo_pre',
            '7mo_pre': '7mo_pre',
            '7mo_Webinar': '7mo_Webinar',
            '9mo_pre': '9mo_pre',
            'U1C': 'U1C',
        },
        'Discover': {
            'Announcement': '1st',
            'Covered': '2nd',
            'Education': '3rd',
            'Coverage': '4th',
            '1SQ': '1st',
            '2SQ': '2nd',
            '3SQ': '3rd',
            '4SQ': '4th',
            'Magnet': 'Magnet',
        },
        'InitialSuite': {
            'GetReadyToEnroll': 'GREG',
            'Get_Ready_Enroll': 'GREG',
            'Intro': 'Intro',
            'GSG': 'Intro',
        },
        'MAV': {
            'birthday': 'bday',
            'bday': 'bday',
        },
        'NonResponder': {
            '-LC': 'LC',
            'NOGC': 'LC',
            'LSGC': 'LC',
            'Non-Responder': 'NonResponder',
            'Time_Running_Out': 'NonResponder',
            '-NON-': 'NonResponder',
            '-CRT-': 'NonResponder',
            '1mo': 'NotTooLate_1mo',
        }
    }
    try:
        match_key_list = list(desc_matching_dict[comm_reporting_type].keys())
        s = process.extractOne(asset_name, match_key_list)
        if s[1] > 70:
            ic(logging.info(
                f'Asset Reporting Description matching compared Asset:"{asset_name}" and "{comm_reporting_type}" which found {s[0]} with {s[1]}% certainty'))
            comm_reporting_desc_match = desc_matching_dict[s[0]]
        else:
            ic(logging.info(
                f'Asset Reporting Description matching compared Asset:"{asset_name}" and "{comm_reporting_type}" which found NO CONFIDENT MATCHES'))
            comm_reporting_desc_match = 'NULL'
    except:
        ic(logging.info(f'Asset Reporting Description matching compared "{asset_name}" and found NO CONFIDENT MATCHES'))
        comm_reporting_desc_match = 'NULL'
    return comm_reporting_desc_match


def run(asset_name: str, reporting_type: str = None, reporting_description: str = None, comm_type_id: int = None):
    # If the asset already exists, return it w/o doing anything else
    conn = sql.sql_connect(server=config.sql_communications_tracking['host'] , database=config.sql_communications_tracking['database'])
    cursor = conn.cursor()
    query = f"SELECT templateId FROM dbo.Templates WHERE templateName = '{asset_name}'"
    return_df = pd.read_sql(query, conn)
    if return_df.shape[0] == 1:
        template_id = return_df.templateId[0]
        return template_id

    if not reporting_type:
        reporting_type = estimate_template_reporting_type(asset_name)
    if not reporting_description:
        reporting_description = estimate_template_reporting_description(asset_name, reporting_type)
    if not comm_type_id:
        comm_type_id = identify_template_typeid(asset_name)
    if (reporting_type == 'NULL') | (reporting_type == 'Needs_Review'):
        make_asset_review_task(asset_name)
    
    ic(logging.info(f'Attempting to Insert Asset [{asset_name}: {reporting_type}, {reporting_description}] into prodCommunicationsTracking'))
    sql_file_loc = f"{config.shared_directory['sql_auto']}/Automation/Tracking"
    file_name = r'ALFRED_Insert_CommunicationsTrackingTemplateId.sql'
    sf = open(os.path.join(sql_file_loc, file_name), 'r')
    sql_text = sf.read()

    asset_replace_dict = {"@@assetName": f"'{asset_name}'",
                          "@@reportingType": f"'{reporting_type}'",
                          "@@communicationTypeId": f"{comm_type_id}",
                          "@@reportingDescription": f"'{reporting_description}'",
                          "'NULL'": 'NULL',
                          }
    for key in asset_replace_dict:
        value = asset_replace_dict[key]
        sql_text = sql_text.replace(key, value)

    try:
        cursor.execute(sql_text)
        inserted_return = cursor.fetchall()
        template_id = inserted_return[0][0]

        cursor.commit()
        conn.close()
        return template_id
    except pyodbc.Error as ex:
        cursor.rollback()
        if ex.args[0] == '23000':  # duplicate asset Insert
            logging.info(ic(str(ex)))
            conn.close()
        else:
            logging.error(ic(str(ex.args[0])))
            conn.close()
    except Exception as e:
        logging.error(ic(e))
        conn.close()


if __name__ == '__main__':
    # asset_name = '510366-101724-TIAA_Allocation_Increase-CC_PRT_1'
    # reporting_type = 'Custom'
    # reporting_description = 'Custom'
    # comm_type_id = 1
    # run(asset_name)
    # asset_name = '720068_email_moo_123554'
    # best_match_asset_type = estimate_template_reporting_type(asset_name)

    # comm_type = identify_template_typeid(asset_name)
    asset_name = 'test'
    make_asset_review_task(asset_name)

    pass
