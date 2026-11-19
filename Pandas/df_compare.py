import pandas as pd
import warnings

def get_different_rows(original_df: pd.DataFrame, new_df: pd.DataFrame, match_column: str = 'campaignSegmentGuid'):
    original_df.drop(columns=['ss_row_id'], axis=1, inplace=True, errors='ignore')
    differences_df = new_df[~new_df.apply(tuple, 1).isin(original_df.apply(tuple, 1))]
    diff_match_list = differences_df[match_column]
    update_df = new_df[~(new_df[match_column].isin(diff_match_list))]
    return update_df


def merge_changes_df(source_df, new_df, match_column='campaignSegmentGuid'):
    shared_cols = list(set(list(source_df.columns)).intersection(set(list(new_df.columns))))
    shared_cols.remove(match_column)
    for index, row in new_df.iterrows():
        warnings.filterwarnings("once")
        seg_guid = row[match_column]
        row_ind_source = source_df.index[source_df[match_column] == seg_guid].tolist()
        # each of the shared columns is updated with the value from the new_data
        for col in shared_cols:
            source_df.loc[:, ('col', 'row_ind_source')] = row[col]

    dropped_df = new_df.drop(columns=shared_cols)
    merged_df = pd.merge(source_df, dropped_df, on=match_column)

    return merged_df


def return_diff_merged(source_df: pd.DataFrame, new_df: pd.DataFrame, match_column: str = 'campaignSegmentGuid'):
    merged_df = merge_changes_df(source_df, new_df, match_column)
    diff_df = get_different_rows(source_df, merged_df)
    diff_df.to_csv('//secureshare/Encrypted Share/Print Communications/Team_Members/David/Personal In-Progress/Temp/__diff_df.csv')
    return diff_df


if __name__ == '__main__':
    source_df = pd.read_csv('//secureshare/Encrypted Share/Print Communications/Team_Members/David/Personal In-Progress/Temp/__source_df.csv')
    new_df = pd.read_csv('//secureshare/Encrypted Share/Print Communications/Team_Members/David/Personal In-Progress/Temp/__new_df.csv')
    ret_df = merge_changes_df(source_df, new_df)
    diff_df = get_different_rows(source_df, ret_df)
    pass