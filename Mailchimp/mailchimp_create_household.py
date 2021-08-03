import pandas as pd
from icecream import ic
import configFolder as config


def mailchimp_household(valid_emails_file_loc: str) -> str:
    """
            Combines record for Mailchimp Households when primary and Dependent Email are the same.
        :param valid_emails_file_loc:
        :return: save_file_loc: str
    """
    print('Starting: mailchimp_household')
    df = pd.read_csv(valid_emails_file_loc)
    df["emailAddress"] = df["emailAddress"].str.lower()
    df.sort_values(by=["isPrimary", "coordinatedMailingId"], axis=0, ascending=False)
    df['eml_count'] = df.groupby(['emailAddress', 'clientName'])['emailAddress'].transform('count')
    household_deps = []
    for index, row in df.iterrows():
        if row['eml_count'] > 1:
            if row['isPrimary'] is True:
                index_dep = index + 1
                row_dep = df.iloc[index_dep, :]
                if row_dep['eml_count'] > 1 and row_dep["emailAddress"] == row["emailAddress"] and str(row_dep['isPrimary'] == 'False') and index_dep == index+1:  # row2['house_count'] > 1 and
                    if str(row["lastName"]).lower == str(row_dep["lastName"]).lower:
                        df.at[index, "firstName"] = str(f'{row["firstName"]} and {row_dep["firstName"]}')

                    else:
                        df.at[index, "firstName"] = str(f'{row["firstName"]} {row["lastName"]} and')
                        df.at[index, "lastName"] = str(f'{row_dep["firstName"]} {row_dep["lastName"]}')
                    household_deps.append(index_dep)
                    row['eml_count'] -= 1
    df = df.drop(labels=household_deps, axis=0)
    df = df.drop(['eml_count'], axis=1)  # 'house_count',
    save_loc = r'C:\Users\davsmi\Documents\Python\Davids Laboratory_stage\TestData_stage\return Data\Householded_valid_emails.csv'
    df.to_csv(save_loc)
    return save_loc


if __name__ == '__main__':
    file_loc = r'C:\Users\davsmi\Documents\Python\Davids Laboratory_stage\TestData_stage\IFP_OngoingAgeInMailFile_TEST.csv'
    ic(mailchimp_household(file_loc))