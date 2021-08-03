""" NOT USED """


import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError

import configFolder.config as config

audience_id = config.mailchimp_settings_dictionary['audience_id']
tag_name = {"name": '2021 06 Test'}
tag_id = '17297'
id='1bada860a8'

try:
    client_mcm = MailchimpMarketing.Client()
    client_mcm.set_config({"api_key": config.mail_chimp["api_key"], "server": config.mail_chimp["server_prefix"]})
    print(client_mcm)
except ApiClientError as error: print("Error: {}".format(error.text))


segment_details = {
    'segment_opts': {
        'conditions': {
            'match': "all",
            'conditions': [f"static_segment='17297'", "SEGMENT='True'"]
            },
        'list_id': audience_id},
    'settings': {
        'subject_line': config.mailchimp_settings_dictionary["IFP_subject_line"],
        'from_name': config.mailchimp_settings_dictionary['from_name'],
        'reply_to': config.mailchimp_settings_dictionary['from_email'],
        'title': f'{tag_name} HRA'}
}

try:
    response = client_mcm.campaigns.update(campaign_id='1bada860a8', body=segment_details, async_req=True)
    print(response)
except ApiClientError as error:
    print("Error: {}".format(error.text))


