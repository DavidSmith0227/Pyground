import main as MAIN
import mailchimp_campaigns as mcc


def run_ifp_ongoing_mailchimp():
    """ RUN ON THE 20th of each month """
    # mailing_type = 'IFP_ONGOING'
    mailing_type = 'Alfred_2021_08_TEST_2'
    mcc.resend_ifp_campaign(mailing_type)


def run_ifp_ongoing_resend_mailchimp():
    """ RUN ON THE 1st of each month """
    mailing_type = 'IFP_ONGOING'
    resend = mcc.resend_ifp_campaign(mailing_type)
    return resend


def run_ACH_mailchimp():
    """ RUN on the 6th of each month """
    mailing_type = 'ACH'
    MAIN.run_mailchimp_process(mailing_type)


if __name__ == '__main__':
    print('*** TEST ***')
    # run_ifp_ongoing_mailchimp
    run_ifp_ongoing_resend_mailchimp()
    # run_ACH_mailchimp()