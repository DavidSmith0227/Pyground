from mailchimp3 import MailChimp, mailchimpclient
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
import mailchimp_functions as mcf
import repo.return_last_month_year as rlmy
import repo.drop_adjustment as da
from icecream import ic
import mailchimp_reporting as mcr

import configFolder.config as config

""" connections & constants """
ic.enable()


try:
    client_mcm = MailchimpMarketing.Client()
    client_mcm.set_config({"api_key": config.mail_chimp["mcm_key"], "server": config.mail_chimp["server_prefix"]})
    response = client_mcm.ping.get()
    ic(client_mcm)
    ic(response)

    user = config.mail_chimp["api_username"]
    key = config.mail_chimp["api_key"]
    client = MailChimp(key, user, timeout=60.0)
    response = client.ping.get()
    ic(client)
    ic(response)
    # return client
except() as e:
    print(e)
except ApiClientError as error:
    print("Error: {}".format(error.text))

audience_id = config.mailchimp_settings_dictionary['audience_id']
""" /connections & constants """


def resend_campaign(mailing_type) -> list:
    # TODO: if schedule date falls on Weekend, hold till monday
    """
        used to resend a MailChimp campaign to those who didnt open.
        :return:
    """
    ifp_resend_confirmation_list = []
    config_list_all = config.mailchimp_campaign_settings[mailing_type][segment_configs_list]
    for config_list in config_list_all:
        if config_list[0] is True:
            is_hra = "_HRA"
        else:
            is_hra = ''
        year = rlmy.return_current_year()
        month = rlmy.return_current_month()
        tag = mailing_type
        tag_name = str(f'Alfred_{year}_{month}_{tag}{is_hra}')
        # tag_name = 'Alfred_2021_08_TEST_2'
        campaign_id = mcr.return_campaign_id_matching_str(tag_name, require_exact=True)
        try:
            resend_response = client_mcm.campaigns.create_resend(campaign_id)
            ifp_resend_confirmation_list.append(resend_response)
            campaign_schedule(resend_response["id"], mailing_type, is_resend=True)
        except ApiClientError as error:
            print("Error on: campaign_resend: {}".format(error.text))
    return ifp_resend_confirmation_list


def campaign_schedule(campaign_id, mailing_type, is_resend: bool = False):
    """
        :param campaign_id:
        :param mailing_type: str
        :param is_resend: [bool (True/False)] -- is this a resend of a previous campaign?
        :return: None
    """
    print('*** campaign_schedule ***')
    nextmonth = rlmy.return_next_month()
    nextmonth_year = rlmy.return_year_of_next_month()
    if is_resend is True:
        day = config.mailchimp_campaign_settings[mailing_type]["resend_day"]
    else:
        day = config.mailchimp_campaign_settings[mailing_type]["send_day"]
    non_weekend_schedule_date = da.modify_holiday_or_weekend_dropdate(f'{nextmonth_year}-{nextmonth}-{day}')
    time = 'T19:00:00.000Z'  # 1:00 PM MST
    schedule_time = str(f"{non_weekend_schedule_date}{time}")
    try:
        response = client_mcm.campaigns.schedule(campaign_id, {"schedule_time": schedule_time})
        print(response)
    except ApiClientError as error:
        print("Error on: campaign_schedule: {}".format(error.text))
    return response


def parse_out_suspect_campaigns_from_file(responses: list, expected_count: int, mailing_type: str):
    """
        identifies if a campaign meets expectations to send
            Campaign ppt count <= expected_count
        :param responses:
        :param expected_count:
        :param mailing_type: str
        :return:
    """
    print('*** parse_out_suspect_campaigns_from_file ***')
    response_list = []
    for response in responses:
        campaign_count = response["recipient_count"]["recipient_count"]
        if campaign_count <= expected_count:
            campaign_id = response["campaign_id"]
            is_resend = response["is_resend"]
            schedule_response = campaign_schedule(campaign_id, mailing_type, is_resend)
            response_list.append(schedule_response)
        else:
            # Email, or logging?
            print(
                f'THROWERROR({response["campaign_name"]}): Campaign had count of {response["recipient_count"]} which is greater than the expected count of: {expected_count}\n')
    return response_list


def get_campaign_details_str(tag_name: str, mailing_type, is_hra: int, is_resend: bool = False):
    """
        provides json string to create a campaign with. constraints based on tag_name
        :param tag_name:
        :param is_hra: bool
        :param is_resend: bool=False
        :return: segment_details
    """
    tag_id = mcf.get_tag_id(tag_name)
    if is_hra == 1 or is_hra == 0:
        """ HRA and Non-HRA Split Mailings, EG: IFP_ONGOING """
        if is_hra == 1:
            hra = "_HRA"
            is_hra_str = 'true'
        elif is_hra == 0:
            hra = ''
            is_hra_str = 'false'
        title = f'{tag_name}{hra}'

        subject_line = config.mailchimp_campaign_settings[mailing_type][hra]['subject_line']
        preview_text = config.mailchimp_campaign_settings[mailing_type][hra]["preview_text"]
        template_id = config.mailchimp_campaign_settings[mailing_type][hra]['template_id']
        conditions = [{
                "condition_type": "StaticSegment",
                "field": "static_segment",
                "op": "static_is",
                "value": tag_id
            }, {
                "condition_type": "TextMerge",
                "field": "SEGMENT",
                "op": "is",
                "value": is_hra_str
            }]

    else:
        """ non-HRA discriminating campaigns, EG: ACH_ONLY """
        hra = ''

        title = f'{tag_name}'
        subject_line = config.mailchimp_campaign_settings[mailing_type]['subject_line']
        preview_text = config.mailchimp_campaign_settings[mailing_type]["preview_text"]
        template_id = config.mailchimp_campaign_settings[mailing_type]['template_id']
        conditions = [{
            "condition_type": "StaticSegment",
            "field": "static_segment",
            "op": "static_is",
            "value": tag_id
            }]

    if is_resend is True:
        title = f'{tag_name}_rnd2{hra}'
        rnd1_campaign = f'{tag_name}{hra}'
        rnd1_campaign_id = mcr.return_campaign_id_matching_str(rnd1_campaign, require_exact=True)
        rnd1_campaign_web_id = client_mcm.campaigns.get(rnd1_campaign_id)["web_id"]
        conditions = [{'condition_type': 'Aim',
                       'field': 'aim',
                       'op': 'noopen',
                       'value': rnd1_campaign_web_id},
                      {'condition_type': 'Aim',
                       'field': 'aim',
                       'op': 'sent',
                       'value': rnd1_campaign_web_id}]

    campaign_details = {
            "type": "regular",
            'content_type': 'template',
            'needs_block_refresh': False,
            'resendable': True,
            'recipients': {
                "list_id": audience_id,
                "segment_opts": {
                    "conditions": conditions,
                    "match": "all"
                },
            },
            "settings": {
                'subject_line': subject_line,
                'preview_text': preview_text,
                'title': title,
                'from_name': config.mailchimp_settings_dictionary['from_name'],
                'reply_to': config.mailchimp_settings_dictionary['from_email'],
                'use_conversation': False,
                'to_name': '*|FNAME|* *|LNAME|*',
                'folder_id': config.mailchimp_campaign_settings[mailing_type]['folder_id'],
                'authenticate': True,
                'auto_footer': False,
                'inline_css': False,
                'auto_tweet': False,
                'fb_comments': True,
                'timewarp': False,
                'template_id': template_id,
                'drag_and_drop': True
            }
        }
    return campaign_details


def mc_create_campaigns(tag_name: str, mailing_type):
    print('*** mc_create_ifp_campaigns ***')
    """
        creates HRA and Non-HRA campaigns for IFP as "Allfred_{year}_{month}_tag_name"
        :param tag_name: str --> should be "IFP_ONGOING"
        :return: list of dicts: [{campaign_id: str, is_resend: bool}]
    """
    responses_dicts = []
    """ [[HRA/Non-resend], [Non-Hra/Non-resend] """
    segment_configs_list = config.mailchimp_campaign_settings[mailing_type]['segment_configs_list']  # , [True, True], [False, True]
    for config in segment_configs_list:
        is_hra = config[0]
        is_resend = config[1]
        data = get_campaign_details_str(tag_name, mailing_type, is_hra, is_resend)
        try:
            response = client_mcm.campaigns.create(body=data)
            responses_dicts.append({"campaign_name": response["settings"]["title"], "campaign_id": response["id"],
                                    "recipient_count": response["recipients"], "is_resend": is_resend})
            ic(responses_dicts)
        except ApiClientError as error:
            print("Error on: mc_create_and_schedule_ifp_campaigns: {}".format(error.text))
    return responses_dicts


def create_TEST_campaign(tag_name):
    """
        :param tag_name:
        :return:
    """
    tag_id = mcf.get_tag_id(tag_name)

    data = {
        "type": "regular",
        'content_type': 'template',
        'needs_block_refresh': False,
        'resendable': True,
        "recipients": {
            "list_id": audience_id,
            "segment_opts": {
                "match": "all",
                "conditions": [{
                    "condition_type": "StaticSegment",
                    "field": "static_segment",
                    "op": "static_is",
                    "value": tag_id
                },
                    {
                        "condition_type": "TextMerge",
                        "field": "SEGMENT",
                        "op": "is",
                        "value": "True"
                    }]
            },
        },
        "settings": {
            'subject_line': "Test Subject line",  # config.mailchimp_settings_dictionary['IFP_subject_line']
            'preview_text': "Test Preview text",  # config.mailchimp_settings_dictionary["IFP_preview_text"]
            'title': tag_name,
            'from_name': "jeremy.suwinski@extendhealth.com",  # config.mailchimp_settings_dictionary['from_name']
            'reply_to': "jeremy.suwinski@extendhealth.com",  # config.mailchimp_settings_dictionary['from_email']
            'use_conversation': False,
            'to_name': '*|FNAME|* *|LNAME|*',
            'folder_id': config.mailchimp_campaign_settings['IFP_folder_id'],
            'authenticate': True,
            'auto_footer': False,
            'inline_css': False,
            'auto_tweet': False,
            'fb_comments': True,
            'timewarp': False,
            'template_id': config.mailchimp_campaign_settings['IFP_template_id'],
            'drag_and_drop': True
        }
    }
    try:
        response = client.campaigns.create(body=data)
        ic(response)
    except ApiClientError as error:
        print("Error_on_create_campaign: ".format(error.text))
    ic(response)
    return response


if __name__ == '__main__':
    print(f'************\n****TEST****\n************')

    # tag_name = mco.make_tag_name('TEST')
    # # tag_name = '2021 06 IFP Test'
    # responses = mc_create_ifp_campaigns(tag_name)
    # # responses = [{'campaign_name': 'Alfred_2021_08_TEST_HRA', 'campaign_id': 'a9b5f9c61c', 'recipient_count': {'list_id': '3070c4f18d', 'list_is_active': True, 'list_name': 'Via Benefits Insurance Services', 'segment_text': '<p class="!margin--lv0 display--inline">Contacts that match <strong>all</strong> of the following conditions:</p><ol id="conditions" class="small-meta text-transform--none"><li class="margin--lv1 !margin-left-right--lv0">Tags contact is tagged <strong>Alfred_2021_08_TEST</strong></li><li class="margin--lv1 !margin-left-right--lv0">Segment is  True</li></ol><span>For a total of <strong></strong> emails sent.</span>', 'recipient_count': 0, 'segment_opts': {'match': 'all', 'conditions': [{'condition_type': 'StaticSegment', 'field': 'static_segment', 'op': 'static_is', 'value': 17697}, {'condition_type': 'TextMerge', 'field': 'SEGMENT', 'op': 'is', 'value': 'True'}]}}, 'is_resend': False}]
    # expected_count = 4
    # parse_out_suspect_campaigns_from_file(responses, expected_count)


    # campaign_schedule('b8294460f9')

    # campaign_resend()
    # campaign_schedule('7db1f00fd3', is_resend=True)
    # client_mcm = mcf.connect_mailchimp()
    # resend_response = client_mcm.campaigns.create_resend('a343bda6d2')
    # print(resend_response)
    campaign_schedule('6bdd613dbc', 'IFP_ONGOING', is_resend=True)
    # TODO:
    #  Resend: # a343bda6d2  TEST_EML_DSMITH
    #  Does Create_Resend work, or do we need to replicate, and then Resend?