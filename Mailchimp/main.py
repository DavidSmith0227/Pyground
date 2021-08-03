import pandas as pd
import numpy as np
import mailchimp3
from datetime import date
import os
import shutil
from icecream import ic

import configFolder.config as config
import mailchimp_functions as mcf
import Mailchimp.mailchimp_campaigns as mcc
import Mailchimp.mailchimp_other as mco
import Mailchimp.mailchimp_reporting as mcr
import Mailchimp.mailchimp_header_merge_updater as mchmu
import Mailchimp.mailchimp_create_household as mchh

import zerobounce_sdk as zb


# import logging

# import repo.dataSftpConnect
# import Mailchimp.zerobounce_sdk

# -- Jeremy Doing this part
# def move_file_to_sun(file_loc):
#     print('*** print_file_combine_and_post ***')
#     # SFTP connect and move to Sun


def final_print_file_combiner(filename):
    # TODO: NEEDS WORK & review
    # I believe Jeremy is working on this as well.
    # # # Not currently being run
    print('*** campaign_find_unsubscribers ***')
    last_months_campaign_ids = mcr.get_last_months_ifp_mailchimp_campaigns()
    non_openers = mcr.return_non_opens_last_month(last_months_campaign_ids)  # only returns CID's
    unsubs = mcr.return_unsubscribers(last_months_campaign_ids)
    unsubs_and_nopens = mcr.combine_unsubs_w_nopen(non_openers, unsubs)
    print_final_save_loc = mco.final_print_file_collation(filename, unsubs_and_nopens)
    print(print_final_save_loc)
    return print_final_save_loc


def create_campaign(tag_name, ppt_count_to_mc, mailing_type):
    """ Campaign """
    campaign_responses = mcc.mc_create_campaigns(tag_name, mailing_type)
    print(campaign_responses)
    schedule_response = mcc.parse_out_suspect_campaigns_from_file(campaign_responses, ppt_count_to_mc, mailing_type)
    return schedule_response


def mc_person_add(mc_ready_file_loc, tag_name):
    """ Audience & Tags """

    batch_add_returns = mcf.batch_add_members(mc_ready_file_loc)
    batches = [batch_add_returns["post_response"], batch_add_returns["put_response"]]
    print(batches)
    batch_return = mcf.get_batch(batches)
    ppt_count_to_mc = len(batch_add_returns["emails"])
    mcf.add_tag_by_hash(batch_add_returns["emails"], tag_name)
    return ppt_count_to_mc


def zerobounce_retrieve_file(filename, send_file_id):
    zb_return_file_loc = zb.zb_get_file(send_file_id, filename)
    to_mc_file_loc = zb.zb_email_splitter(zb_return_file_loc, filename)
    return to_mc_file_loc


def zerobounce_run(mailing_type) -> dict:
    ifp_file = zb.find_file(mailing_type)
    zb_files_directory = ifp_file["file_directory"]
    filename = ifp_file["filename"]

    file_split = zb.zb_split_email_rows_from_file(zb_files_directory, filename)
    email_file_loc = file_split["email_file_loc"]
    print_file_loc = file_split["print_file_loc"]
    """ Maybe dont need print_file_loc"""
    address_column_int = config.mailchimp_campaign_settings[mailing_type]['address_column_int']
    zb_send_file_id = zb.zb_send_file(email_file_loc, address_column_int)
    return {'email_file_loc': email_file_loc, 'filename': filename, 'send_file_id': zb_send_file_id}


def run_mailchimp_process(mailing_type):
    mailing_type = mailing_type

    """ZB"""
    zb_return = zerobounce_run(mailing_type)
    filename = zb_return["filename"]
    send_file_id = zb_return["send_file_id"]
    zb_valid_file_loc = zerobounce_retrieve_file(filename, send_file_id)
    zb.zb_get_credits()

    """ TEST data for MC"""
    # mc_ready_file_loc = r'C:\Users\davsmi\Documents\Python\Davids Laboratory_stage\TestData_stage\zeroBounce return files\TEST_MultiClient_ACHTransitionMailFile_CLDS-0227-07-21_final.csv_VALID.csv'
    # filename = 'TEST_MultiClient_ACHTransitionMailFile_CLDS-0227-07-21_final.csv'

    """MC"""
    mc_merge_file_loc = mchmu.update_df_header_to_merge_tags(zb_valid_file_loc)
    mc_householded_merge_file_loc =  mchh.mailchimp_household(mc_merge_file_loc)
    tag_name = mco.make_tag_name(mailing_type)
    mc_person_add_count = mc_person_add(mc_householded_merge_file_loc, tag_name)
    create_campaign(tag_name, mc_person_add_count, mailing_type)

    """ SFTP to SUN """


if __name__ == '__main__':
    print("*** TEST ***")
    mailing_type = 'ACH'
    # mailing_type = 'IFP_ONGOING'
    run_mailchimp_process(mailing_type)
    '''Run IFP_ONGOING 20th each month'''
    '''Run Main_resend 1st of each month'''  # THIS SCRIPT --> \main_resend.py

    '''Run ACH 6th of each month'''



