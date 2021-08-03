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

import zerobounce_sdk as zb

import repo.dataSftpConnect
import Mailchimp.zerobounce_sdk
# import logging


def run_campaign_resend(mailing_type):
    resend = mcc.resend_campaign(mailing_type)
    return resend


if __name__ == '__main__':
    mailing_type = 'IFP_ONGOING'  # ACH currently does not schedule resends.
    ifp_resend = run_campaign_resend(mailing_type)
    print(ifp_resend)

    #
    # mcr.return_campaign_id_matching_str('2021 July IFP Ongoing')
    # mcr.return_campaign_id_matching_str('2021 July IFP Ongoing HRA')