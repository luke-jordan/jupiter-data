#!/usr/bin/env python
import os

from json import loads
from datetime import datetime

import pandas as pd
from google.cloud import storage, bigquery

pd.options.mode.chained_assignment = None  # default='warn'

unit_convertors = { 'WHOLE_CURRENCY': 1, 'WHOLE_CENT': 100, 'HUNDREDTH_CENT': 10000 }

def extract_save_requirement(parsed_context):
    if 'statusConditions' not in parsed_context:
        return None, None
    
    # we look for the first
    conditions = parsed_context['statusConditions']
    save_type = None
    save_threshold = None
    
    sought_conditions = ['save_greater_than', 'first_save_above', 'balance_crossed_major_digit', 'balance_crossed_abs_target']
    is_save_condition = lambda cond: len([check for check in sought_conditions if cond.startswith(check)]) > 0
    
    for value in conditions.values():
        matches = [cond for cond in value if is_save_condition(cond)]
        if (len(matches) == 0):
            continue
            
        condition_clause = matches[0]
        save_type = condition_clause[0:condition_clause.find(' ')]
        
        param_start = condition_clause.find('{') + 1
        param_end = condition_clause.find('}')
        save_parameter = condition_clause[param_start:param_end].split('::')
        
        save_threshold = int(save_parameter[0]) / unit_convertors[save_parameter[1]]
                
    return save_type, save_threshold

# ## Load in the master DF
def assemble_dataset(master_df):
    master_df['user_id_count'] = master_df.groupby(['user_id'])['time_transaction_occurred'].transform('count')
    
    # we remove the top 2, because they are team members often testing, so distort
    outlier_user_ids = master_df['user_id'].value_counts()[:2].index.tolist()
    # probably a better panda-ninja way to do this but not worth it right now
    for user_id in outlier_user_ids:
        master_df = master_df[master_df.user_id != user_id]

    print('DF head now: ', master_df.head())
    print('Dataframe shape: ', master_df.shape)

    # Extract primary boost info

    bdf = master_df[master_df['event_type'].str.contains('BOOST_CREATED')]
    
    bdf["parsed_context"] = bdf.context.apply(loads) 

    bdf["boost_id"] = bdf["parsed_context"].apply(lambda context: context["boostId"])
    bdf["boost_type"] = bdf["parsed_context"].apply(lambda context: context["boostType"])
    bdf["boost_category"] = bdf["parsed_context"].apply(lambda context: context["boostCategory"])

    bdf["boost_time"] = pd.to_datetime(bdf["time_transaction_occurred"], unit='ms')


    print('Core boost properties extracted: ', bdf.head())

    bdf['boost_amount'] = bdf['parsed_context'].apply(lambda context: int(context['boostAmount']) / unit_convertors[context['boostUnit']])

    bdf['save_requirements'] = bdf['parsed_context'].apply(extract_save_requirement)
    bdf[['save_type', 'save_amount']] = pd.DataFrame(bdf['save_requirements'].tolist(), index=bdf.index)
    bdf["boost_save_ratio"] = bdf["save_amount"] / bdf["boost_amount"]

    # might use this in the future
    # days_open = (example_context['boostEndTime'] - example_context['boostStartTime']) / (24 * 60 * 60 * 1000)

    bdf["day_of_month"] = bdf["boost_time"].dt.day
    bdf["day_of_week"] = bdf["boost_time"].dt.dayofweek

    return bdf


def merge_with_saves(bdf, master_df):
    sdf = master_df[master_df['event_type'].str.contains('SAVING_PAYMENT_SUCCESSFUL')]
    sdf["save_time"] = pd.to_datetime(sdf["time_transaction_occurred"], unit='ms')

    print('Prior save shape: ', sdf.shape)

    count_prior_saves = lambda boost_row: len(sdf[(sdf["save_time"] < boost_row["boost_time"]) & (sdf["user_id"] == boost_row["user_id"])])

    print('Shape of boost DF prior to checking saves: ', len(bdf))

    bdf["prior_save_count"] = bdf.apply(count_prior_saves, axis = 1)
    print('Assembled with save count, value counts: ', bdf.prior_save_count.describe())

    # ## Extract only boosts where saves are required (maybe move earlier)
    bdf = bdf[bdf.save_type.notna()]

    # ## Extract labels

    # find_next_save = lambda boost_row: len(sdf[(sdf["save_time"] < boost_row["boost_time"]) & (sdf["user_id"] == boost_row["user_id"])])
    def find_next_save(boost_row, time_threshold = 48):
        user_mask = sdf["user_id"] == boost_row["user_id"]
        save_time_mask = sdf["save_time"] > boost_row["boost_time"]
        duration_mask = (sdf["save_time"] - boost_row["boost_time"]).astype('timedelta64[h]') < 48
        next_save_df = sdf[user_mask & save_time_mask & duration_mask]
        return len(next_save_df) > 0
    
    bdf["save_within_48"] = bdf.apply(find_next_save, axis=1)

    return bdf

def upload_frame(feature_df):
    gcpgs = storage.Client()

    bucket_name = os.getenv('DATASET_STORAGE_BUCKET', 'prod_boost_ml_datasets')
    file_prefix = os.getenv('DATASET_FILE_PREFIX', 'boost_induce_full_extract')

    bucket = gcpgs.get_bucket(bucket_name)

    file_name = f"{file_prefix}_{datetime.today().strftime('%Y_%m_%dT%H:%M:%S')}.csv"
    
    print(f"Uploading dataframe to bucket {bucket_name}, folder boost_target, file: {file_name}")
    bucket.blob(f"boost_target/{file_name}").upload_from_string(feature_df.to_csv(index=False), 'text/csv')
    print("Completed uploading")

def produce_boost_save_dataset():
    bq = bigquery.Client()
    master_df = bq.query("""
        select * from ops.all_user_events where event_type like 'BOOST_CREATED%' or event_type = 'SAVING_PAYMENT_SUCCESSFUL'
    """).to_dataframe()
    print("Obtained master DF, shape: ", master_df.shape)
    bdf = assemble_dataset(master_df)
    bdf = merge_with_saves(bdf, master_df)
    print(bdf.save_within_48.value_counts())

    final_feature_list = [
        "boost_type",
        "boost_category",
        "save_type",
        "save_amount",
        "boost_amount",
        "boost_save_ratio",
        "day_of_month",
        "day_of_week",
        "prior_save_count",
        "save_within_48"
    ]

    feature_df = bdf[final_feature_list]
    print("Final dataframe shape: ", feature_df.shape)

    upload_frame(feature_df)
    print("Complete")

if __name__ == '__main__':
    produce_boost_save_dataset()
