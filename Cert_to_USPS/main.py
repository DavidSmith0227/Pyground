import pandas as pd


def file_transform(file_repo, cert_file):
    # file_repo = r'C:\Users\davsmi\Documents\Python\Davids Laboratory\Cert_to_USPS'
    # cert_file = r'\CertifiedDaily_06-08-2021.csv'
    save_file_name = cert_file.replace('CertifiedDaily', 'USPSCertLetterStatus')
    save_file_name = save_file_name.replace('-', '_')
    save_file_name = save_file_name.replace(".csv", str("_Transformed.csv"))

    cert = pd.read_csv(str(file_repo + cert_file))
    print(cert)

    cert = cert.rename(columns={'ToReference': 'Client Code',
                                'PIC': 'PIC - Tracking #',
                                'ToName': 'First Name',
                                'ToCity': 'City',
                                'ToState': 'State/Province',
                                'ToZip': 'Postal/Zip Code',
                                'DateCreated': 'Date Of Mailing',
                                'SignatureDoc': 'Delivery Status',
                                'DeliveryDate': 'Delivery Date'})

    drop_list = ['GroupName', 'FromZip', 'MailType', 'FinalPostage', 'CostOfService', 'TrackingStatus', 'SignatureName']
    for column in drop_list:
        cert = cert.drop(column, axis=1)

    column_adds = ['Last Name', 'Address 1', 'Address 2', 'Class Of Mail', 'Delivery Time', 'Delivery Days',
                   'Delivery Business Days', 'EMail Address 1', 'EMail Address 2'
                   ]
    for column in column_adds:
        cert[column] = None

    # # Reorder columns to match USPS order
    cert = cert[[
        'Client Code',
        'PIC - Tracking #',
        'First Name',
        'Last Name',
        'Address 1',
        'Address 2',
        'City',
        'State/Province',
        'Postal/Zip Code',
        'Date Of Mailing',
        'Class Of Mail',
        'Delivery Status',
        'Delivery Date',
        'Delivery Time',
        'Delivery Days',
        'Delivery Business Days',
        'EMail Address 1',
        'EMail Address 2'
    ]]

    cert.to_csv(file_repo + save_file_name, index=False)
    print(f'File saved as: {save_file_name}')


if __name__ == '__main__':
    # file_repo = r'C:\Users\davsmi\Documents\Python\Davids Laboratory\Cert_to_USPS'
    # cert_file = r'\CertifiedDaily_06-08-2021.csv'
    file_transform(file_repo, cert_file)
    # file_transform("john F Smith")
