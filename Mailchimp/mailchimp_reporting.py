import datetime

from mailchimp3 import MailChimp
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from icecream import ic

import configFolder.config as config
import mailchimp_functions as mcf

""" connections & constants """
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
except ApiClientError as error:
    print("Error Connecting(mailchimp_reporting): {}".format(error.text))

audience_id = config.mailchimp_settings_dictionary['audience_id']
""" /connections & constants """

# TODO: Pull "not-imported" and "invalid" emails from MAILCHIMP into the mail file as well.


def return_campaign_id_matching_str(campaign_search_str: str, require_exact: bool = False):
    print('*** return_campaign_id_matching_str ***')
    all_campaign_ids = []
    try:
        campaign_search = client_mcm.searchCampaigns.search(campaign_search_str)
        results = campaign_search['results']
        for result in results:
            ic(result)
            campaign_id = result['campaign']['id']
            title = result['campaign']['settings']['title']
            if require_exact is True:
                exact_match = (title.lower() == campaign_search_str.lower())
                if exact_match is True:
                    return campaign_id
                else:
                    continue
            if campaign_search_str.find(' ') > 0:
                campaign_search_split = campaign_search_str.split(' ')
            elif campaign_search_str.find('_') > 0:
                campaign_search_split = campaign_search_str.split('_')
            else:
                campaign_search_split = [campaign_search_str]
            # TODO: error handling
                # TODO: improve Fuzzy search
                if campaign_search_split[0] in title and campaign_search_split[1] in title and campaign_search_split[2] in title:
                    all_campaign_ids.append(f"{campaign_id}")

            # all_campaign_ids.append(f"{title}:{campaign_id}")
            # if campaign_search_split[:] in title:
            # if campaign_search_split[0] in title and campaign_search_split[1] in title and campaign_search_split[2] in title:
            #     all_campaign_ids.append(f"{campaign_id}")
    except ApiClientError as error:
        print("Error: {}".format(error.text))
    return all_campaign_ids


def get_last_months_ifp_mailchimp_campaigns():
    print('*** get_last_months_ifp_mailchimp_campaigns ***')
    year = datetime.date.today().strftime("%Y")
    month = datetime.date.today().strftime("%B")
    # TODO: this needs to by last-months Tag_names (HRA / non_hra)
    campaign_name_search = f'{year} {month} IFP'
    campaign_ids = return_campaign_id_matching_str(campaign_name_search, require_exact=True)
    return campaign_ids


# TODO: is this limited by 1000 per campaign?
# Function is WORKING: NEEDS REVIEW
def get_campaign_non_open_report(campaign_id):
    open_details_response = client_mcm.reports.get_campaign_open_details(campaign_id, count=1000)
    non_open_list = []
    open_list = []
    for members in open_details_response["members"]:
        cid = members["merge_fields"]["MCID"]
        if members["opens_count"] == 0:
            non_open_list.append(cid)
        else:
            open_list.append(cid)
            continue
    # print(f'member_count: {member_count}\nnon_open_list: {non_open_list}\nopen_list: {open_list}')
    return non_open_list
    # return non_open_list # # correct value to pull, pulling open_list for testing


def return_non_opens_last_month(campaigns):
    total_non_opens = []
    for campaign_id in campaigns:
        non_open_list = get_campaign_non_open_report(campaign_id)
        for non_opener in non_open_list:
            if non_opener not in total_non_opens:
                total_non_opens.append(non_opener)
    return total_non_opens


def get_campaign_unsubscribers(campaign_id: str):
    unsubscribers_list = []
    try:
        unsub_response = client_mcm.reports.get_unsubscribed_list_for_campaign(campaign_id)
        for members in unsub_response["unsubscribes"]:
            cid = members["merge_fields"]["MCID"]
            unsubscribers_list.append(cid)
    except ApiClientError as error:
        print("Error(get_campaign_unsubscribers): {}".format(error.text))
    return unsubscribers_list


def return_unsubscribers(campaigns: list):
    total_unsubscribes = []
    for campaign_id in campaigns:
        unsubscribe_list = get_campaign_unsubscribers(campaign_id)
        for unsubscriber in unsubscribe_list:
            if unsubscriber not in total_unsubscribes:
                total_unsubscribes.append(unsubscriber)
    return total_unsubscribes


def combine_unsubs_w_nopen(nopens, unsubs):
    """
        gathers CID list from non-open, and unsub lists
        :param nopens:
        :param unsubs:
    :return:
    """
    list = []
    for cid in nopens:
        list.append(cid)
    for cid in unsubs:
        if cid not in list:
            list.append(cid)
    return list


if __name__ == '__main__':
    print(f'************\n****TEST****\n************')

    # campaign_id = return_campaign_id_matching_str('2020 Fall Newsletter OPEN ECO Drop 1', require_exact=True)
    # campaign_details = mcf.get_campaign(campaign_id)
    # print(campaign_details)




    # campaigns = get_last_months_ifp_mailchimp_campaigns()
    # non_opens = return_non_opens_last_month(campaigns)
    # unsubscribers = return_unsubscribers(campaigns)
    # cuwn = combine_unsubs_w_nopen(non_opens, unsubscribers)
    # ic(cuwn)

    # 13d76f205a - 2020 Fall Medicare Newsletter
    campaign_id = '13d76f205a'
    get_campaign_non_open_report(campaign_id)
    opens = get_campaign_non_open_report(campaign_id)
    unsubs = get_campaign_unsubscribers(campaign_id)
    cuwn = combine_unsubs_w_nopen(nopens, unsubs)
    print(cuwn)