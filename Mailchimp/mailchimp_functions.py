import json
import pprint
import time
import hashlib

import pandas as pd
from mailchimp3 import MailChimp
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError

import configFolder.config as config
from icecream import ic
import mailchimp_other as mco

""" connections & constants """
try:
    client_mcm = MailchimpMarketing.Client()
    client_mcm.set_config({"api_key": config.mail_chimp["mcm_key"], "server": config.mail_chimp["server_prefix"]})
    response = client_mcm.ping.get()
    print(f'{response}')
except ApiClientError as error:
    print("Error: {}".format(error.text))
try:
    user = config.mail_chimp["api_username"]
    key = config.mail_chimp["api_key"]
    client = MailChimp(key, user, timeout=60.0)
    response = client.ping.get()
    print(f'{response}')
    # return client
except() as e:
    print(e)

audience_id = config.mailchimp_settings_dictionary['audience_id']
""" /connections & constants """


def add_tag_by_hash(emails: list, tag_name: str):
    """
        "<Response [204]>" == Process worked, but doesnt have anything to return.
        :param emails:
        :param tag_name:
        :return:
    """
    tag_detail = {'name': tag_name, 'status': 'active'}
    for email in emails:
        email_lower = str(email).lower()
        hash = (hashlib.md5(email_lower.encode('utf-8')).hexdigest())
        try:
            tag_item = client_mcm.lists.update_list_member_tags(list_id=audience_id,
                                                                subscriber_hash=hash,
                                                                body={"tags": [tag_detail]})
            print(str(f'{email} was submitted for tags: {tag_detail} and received code: {tag_item} '))
        except ApiClientError as error:
            print(f'Error(add_tag_by_hash): {error.text}\nemail: {email}\ntag_detail: {[tag_detail]}')


def get_batch(member_batch_return: list):
    print('*** get_batch ***')
    batch_response = []
    time.sleep(5)
    member_batch_return = list(filter(None, member_batch_return))
    for batch in member_batch_return:
        batch = batch["id"]
        attempts = 1
        print(f'batch_id: {batch}')
        success_check = client.batch_operations.get(batch_id=batch)
        while success_check["status"] != 'finished' and attempts < 30:
            success_check = client.batch_operations.get(batch_id=batch)
            attempts += 1
            time.sleep(attempts * 2)
        if success_check["status"] == 'finished':
            response = success_check
            batch_response.append(response)
            print(batch_response)
    return batch_response


def batch_add_members(valid_emails_file_loc: str):
    """
    Send file to Mailchimp to add participants
        :param valid_emails_file_loc: str: file_location
        :returns: {'post_response', 'put_response', 'emails'}
    """
    print('*** batch_add_members ***')
    df = pd.read_csv(valid_emails_file_loc, dtype=str)
    put_operations = []
    post_operations = []
    emails = []

    for index, row in df.iterrows():
        """REQUIRED MERGE FIELDS"""
        email_address = df.at[index, "emailaddress"]
        MCID = df.at[index, "personid"]
        FNAME = df.at[index, "firstname"]
        LNAME = df.at[index, "lastname"]
        FULLNAMES = f'{df.at[index, "lastname"]} {df.at[index, "firstname"]}'
        MCLIENT = df.at[index, "clientname"]
        MHOURS = df.at[index, "businesshours"]
        MPHONE = df.at[index, "clientcallcenterphonenumber"]

        """OPTIONAL MERGE FIELDS"""
        # TODO: Add "isFunded" as a Merge Tag
        try:
            SEGMENT = df.at[index, "SEGMENT"]
        except:
            SEGMENT = ''
        try:
            MURL = df.at[index, "MURL"]
        except:
            MURL = ''
        try:
            PLANTYPENA = df.at[index, "PLANTYPENA"]
        except:
            PLANTYPENA = ''
        try:
            EDATE = df.at[index, "EDATE"]
        except:
            EDATE = ''

        emails.append(email_address)
        # TODO: test - is "status: "subscribed" overriding any unsubs / other?
        # # TODO: ANSWER: YES
        databody_item = ({
            'email_address': email_address,
            'status': "subscribed",
            'merge_fields': {
                "MCID": MCID,
                "FNAME": FNAME,
                "LNAME": LNAME,
                "FULLNAMES": FULLNAMES,
                "EDATE": EDATE,
                "MCLIENT": MCLIENT,
                "MURL": MURL,
                "MHOURS": MHOURS,
                "MPHONE": MPHONE,
                "PLANTYPENA": PLANTYPENA,
                "SEGMENT": SEGMENT
            }})
        hash = (hashlib.md5(email_address.encode('utf-8')).hexdigest())
        if list_members(hash) == 'None':
            # POST: Adds the person
            # databody_item['status'] = "subscribed"
            operation_item = {"method": "POST", "path": str(f"/lists/{audience_id}/members/"),
                              "body": json.dumps(databody_item)}  # PUT for update, Post for new?
            # post_response = client_mcm.batches.start(body={"operations": [operation_item]})
            # print(post_response)
            post_operations.append(operation_item)
            # put_operations.append(operation_item)
        # TODO: elif list_member(hash).status == 'unsubscribed' # Add back to mail file.
        else:
            # PUT / Patch: Updates an existing person
            databody_item['status_if_new'] = databody_item.pop("status")
            operation_item = {"method": "PATCH", "path": str(f"/lists/{audience_id}/members/{email_address}"),
                              "body": json.dumps(databody_item)}  # PUT for update, Post for new?
            # put_response = client_mcm.batches.start(body={"operations": [operation_item]})
            # print(put_response)
            put_operations.append(operation_item)
            # post_operations.append(operation_item)
    connect_mailchimp()
    if post_operations != []:
        post_response = client_mcm.batches.start(body={"operations": post_operations})
        # post_response = client.batches.create(data={"operations": post_operations})
    else:
        post_response = None
    if put_operations != []:
        put_response = client_mcm.batches.start(body={"operations": put_operations})
        # put_response = client.batches.create(data={"operations": put_operations})
    else:
        put_response = None
    print('moo')
    return {'post_response': post_response, 'put_response': put_response, 'emails': emails}


def list_members(email_hash):
    """
        Provide member details via email_hashes
        :return: member details
    """
    list_id = config.mailchimp_settings_dictionary["audience_id"]
    try:
        members = client.lists.members.get(list_id=list_id, subscriber_hash=email_hash, get_all=False)
    except:
        members = 'None'
    return members


def get_tag_id(tag_name: str):
    """
        returns the primary_key for a tag via tag_name search
        :param tag_name: str
        :return: tag_id
    """
    if isinstance(tag_name, list):
        tag_name = tag_name[0]["name"]
    elif isinstance(tag_name, dict):
        tag_name = tag_name["name"]
    tag_get = client_mcm.lists.tag_search(list_id=audience_id, name=tag_name)
    tag_id = tag_get["tags"]
    return tag_id[0]["id"]


def list_lists():
    """
        Used for research
        :return: None
    """
    list_id = config.mailchimp_settings_dictionary["audience_id"]
    members = client.lists.all(list_id=list_id, subscriber_hash='', get_all=False)
    print(members)


def get_all_campaigns():
    """
        Used for research - better search by str is mailchimp_reporting.return_campaign_id_matching_str
        :return: None
    """
    campaigns = client.campaigns.all(list_id=audience_id, get_all=False, count=10)
    # campaigns = client.campaigns.get(campaign_id='06eefdd836')  #, fields=('id', 'webid', 'list_name')
    # campaigns = client_mcm.campaigns.list(list_id=audience_id, since_create_time='2021-06-11T00:41:47+00:00')
    # campaigns = client.campaign_folders.get(id='8312b9dbc3')
    print(campaigns)


def get_campaign(campaign_id):
    """
        Pull campaign details via Campaign_id
        :param campaign_id:
        :return: Campaign_response
    """
    print(f'get_campaign({campaign_id})')
    response = client.campaigns.get(list_id=audience_id, campaign_id=campaign_id)
    ic(response)
    return response


def get_segment():
    """
        used for research of segments.
        :return: None
    """
    segment_response = client.lists.segments.all(get_all=False, list_id=audience_id,
                                                 count=25)  # , name='2021 June Ongoing IFP HRA'
    print(segment_response)
    return segment_response


def connect_mailchimp():
    user = config.mail_chimp["api_username"]
    key = config.mail_chimp["api_key"]
    try:
        client_mcm = MailchimpMarketing.Client()
        client_mcm.set_config({"api_key": key, "server": config.mail_chimp["server_prefix"]})
        response = client_mcm.ping.get()
        print(f'{response}')
    except ApiClientError as error:
        print("Error: {}".format(error.text))
    try:
        client = MailChimp(key, user, timeout=60.0)
        response = client.ping.get()
        print(f'{response}')
    except() as e:
        print(e)
    audience_id = config.mailchimp_settings_dictionary['audience_id']
    return {'client_mcm': client_mcm, 'client': client, 'audience_id': audience_id}


if __name__ == '__main__':
    print(f'\n*****test*****\n')
    connect_mailchimp()

    file_loc = r'C:\Users\davsmi\Documents\Python\Davids Laboratory_stage\TestData_stage\Original data\merge_file_test.csv'

    batch_add_returns = batch_add_members(file_loc)
    batches = [batch_add_returns["post_response"], batch_add_returns["put_response"]]
    # print(batches)

    # TEST BATCH ID: '839e84fd5c',
    batch_return = get_batch(batches)
    print(batch_return)