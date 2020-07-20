# Does the work of training

import json
from datetime import datetime

import pandas as pd

from google.cloud import bigquery

from sklearn import svm
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import precision_recall_fscore_support, accuracy_score 
from sklearn.preprocessing import OneHotEncoder

client = bigquery.Client()

PROJECT_ID = 'jupiter-production-258809'

# ############################################################
# FEATURE ENGINEERING SECTION ################################
# ############################################################

def obtain_boosts_with_saves():
    sql = f"""
    with boost_offers as (
            select *, TIMESTAMP_MILLIS(created_at) as creation_timestamp 
            from {PROJECT_ID}.ops.all_user_events 
            where event_type like 'BOOST_CREATED%'

    ), save_events as (
            select *, TIMESTAMP_MILLIS(created_at) as creation_timestamp 
            from {PROJECT_ID}.ops.all_user_events 
            where event_type = 'SAVING_PAYMENT_SUCCESSFUL'
    )
    select boost_offers.user_id, boost_offers.event_type, boost_offers.context, 
        boost_offers.creation_timestamp as boost_creation_time, save_events.creation_timestamp as save_completion_time,  
        TIMESTAMP_DIFF(save_events.creation_timestamp, boost_offers.creation_timestamp, HOUR) as time_from_boost_to_save
    from boost_offers left join save_events on boost_offers.user_id = save_events.user_id
    """
    
    df = client.query(sql).to_dataframe()
    return df


def obtain_boosts_with_prior_redemptions():
    sql = f"""
    with boost_offers as (
            select *, TIMESTAMP_MILLIS(created_at) as creation_timestamp 
            from {PROJECT_ID}.ops.all_user_events 
            where event_type like 'BOOST_CREATED%'

    ), boost_redemptions as (
            select *, TIMESTAMP_MILLIS(created_at) as creation_timestamp 
            from {PROJECT_ID}.ops.all_user_events 
            where event_type = 'BOOST_REDEEMED'
    )
    select boost_offers.user_id, boost_offers.event_type, boost_offers.context, 
        boost_offers.creation_timestamp as boost_creation_time, boost_redemptions.creation_timestamp as boost_redemption_time,  
        TIMESTAMP_DIFF(boost_redemptions.creation_timestamp, boost_offers.creation_timestamp, HOUR) as time_from_boost_to_last_redeem
    from boost_offers left join boost_redemptions on boost_offers.user_id = boost_redemptions.user_id
        where TIMESTAMP_DIFF(boost_redemptions.creation_timestamp, boost_offers.creation_timestamp, HOUR) < 0 or
        TIMESTAMP_DIFF(boost_redemptions.creation_timestamp, boost_offers.creation_timestamp, HOUR) is null
    """

    df = client.query(sql).to_dataframe()
    return df


def parse_context_and_set_boost_id(df):
    # extract a bunch of context from the boosts    
    df["parsed_context"] = df.context.apply(json.loads)
    df["boost_id"] = df["parsed_context"].apply(lambda context: context["boostId"])
    # and this functions as our index     
    df["boost_user_id"] = df["boost_id"] + "::" + df["user_id"]
    return df

def extract_prior_save_counts(prior_save_counts):
    print('Past rows: ', prior_save_counts.shape)
    prior_save_counts["boost_prior_saves"] = prior_save_counts.groupby('boost_user_id').transform('count')["save_completion_time"]
    prior_save_counts = prior_save_counts[["boost_user_id", "boost_prior_saves"]]
    prior_save_counts = prior_save_counts.groupby("boost_user_id").first() # no need for a sort
    return prior_save_counts

def extract_time_since_latest_save(prior_save_df):
    with_latest_save = prior_save_df.sort_values("save_completion_time").groupby("boost_user_id", as_index = False).last()
    with_latest_save["days_since_latest_save"] = abs(with_latest_save["time_from_boost_to_save"] / 24)
    with_latest_save = with_latest_save[["boost_user_id", "days_since_latest_save"]]
    return with_latest_save

def extract_time_since_first_save(prior_save_df):
    # for some reason if index, causes issues here
    with_earliest_save = prior_save_df.sort_values("save_completion_time").groupby("boost_user_id", as_index = False).first()
    with_earliest_save["days_since_first_save"] = abs(with_earliest_save["time_from_boost_to_save"] / 24)
    with_earliest_save = with_earliest_save[["boost_user_id", "days_since_first_save"]]
    return with_earliest_save

def extract_prior_redemption(df):
    df = parse_context_and_set_boost_id(df)
    print('Priors, length: ', df.shape)
    adjusted_df = df.sort_values("time_from_boost_to_last_redeem").groupby("boost_user_id", as_index=False).last()
    adjusted_df["has_prior_redeemed"] = adjusted_df.boost_redemption_time.notna()
    return adjusted_df[["boost_user_id", "has_prior_redeemed"]]

def clean_up_and_construct_labels(boosts_with_saves, boosts_with_prior_redeemed):
    unit_convertors = { 'WHOLE_CURRENCY': 1, 'WHOLE_CENT': 100, 'HUNDREDTH_CENT': 10000 }
    
    df = boosts_with_saves
    print('Starting count: ', df.shape)
    
    df['user_id_count'] = boosts_with_saves.groupby(['user_id'])['boost_creation_time'].transform('count')
    
    # we remove the top 2, because they are team members often testing, so distort
    outlier_user_ids = df['user_id'].value_counts()[:2].index.tolist()
    # probably a better panda-ninja way to do this but not worth it right now
    for user_id in outlier_user_ids:
        df = df[df.user_id != user_id]
        
    print('With outlier top users stripped: ', df.shape)
    
    df = parse_context_and_set_boost_id(df)
    
    # here we have our label
    df["is_save_within_day"] = df["time_from_boost_to_save"] < 24
    
    df["boost_amount_whole_currency"] = df["parsed_context"].apply(
        lambda context: int(context["boostAmount"]) / unit_convertors[context["boostUnit"]])
    
    df["boost_type"] = df["parsed_context"].apply(lambda context: context["boostType"])
    df["boost_category"] = df["parsed_context"].apply(lambda context: context["boostCategory"])
    df["boost_type_category"] = df["boost_type"] + "::" + df["boost_category"]
    
    df["day_of_month"] = df["boost_creation_time"].dt.day
    df["hour_of_day"] = df["boost_creation_time"].dt.hour
    df["day_of_week"] = df["boost_creation_time"].dt.dayofweek
    
    # then we construct our future and past masks, calculate prior saves, and find next save
    prior_save_mask = df["time_from_boost_to_save"] < 0
    future_save_mask = df["time_from_boost_to_save"] > 0
        
    # likely a way to do these more simply, but for now doing groups & sorts differently    
    prior_save_counts = extract_prior_save_counts(df[prior_save_mask].copy())
    days_since_latest_save = extract_time_since_latest_save(df[prior_save_mask].copy())
    days_since_first_save = extract_time_since_first_save(df[prior_save_mask].copy())
    
    # then we discard the past
    with_future_saves = df[future_save_mask].copy()
    with_next_save = with_future_saves.sort_values("save_completion_time").groupby("boost_user_id").first()
    
    print('Now with just future saves crossed: ', with_future_saves.shape, ' and next save only: ', with_next_save.shape)
    
    with_prior_redemption = extract_prior_redemption(boosts_with_prior_redeemed)
    
    # and finally we strip out the surplus boost-save pairs (by retaining only the opening)
    # at the moment an inner join, but we may want to turn this into joining from those with saves
    final_df = pd.merge(with_next_save, prior_save_counts, on='boost_user_id')
    final_df = pd.merge(final_df, days_since_latest_save, on='boost_user_id')
    final_df = pd.merge(final_df, days_since_first_save, on='boost_user_id')
    print("And finally, stripped to just one per: ", final_df.shape)
    
    final_df = pd.merge(final_df, with_prior_redemption, on='boost_user_id')
    print("And now with boolean on prior redemption: ", final_df.shape)
    
    return final_df

def feature_extraction(data):
    features_of_interest = [
        "boost_amount_whole_currency", 
        "day_of_month", 
        "boost_prior_saves",
        "boost_type_category",
        "days_since_latest_save",
        "has_prior_redeemed",
        "days_since_first_save",
        "day_of_week",
        "is_save_within_day"
    ]
    stripped_df = data[features_of_interest]
    return stripped_df

#  some painful stuff happening with some categories being missing and then one-hot goes badly wrong
def ensure_all_one_hots(df):
    all_boost_type_categories = [
        'GAME::CHASE_ARROW', 
        'GAME::DESTROY_IMAGE',
        'GAME::TAP_SCREEN', 
        'SIMPLE::ROUND_UP',
        'SIMPLE::SIMPLE_SAVE', 
        'SIMPLE::TIME_LIMITED',
        'SIMPLE::TARGET_BALANCE',
        'SOCIAL::FRIENDS_ADDED',
        'SOCIAL::NUMBER_FRIENDS',
    ]
    
    assignment_args = {}
    for category in all_boost_type_categories:
        column_name = f'boost_type_category_{category}'
        if column_name not in df:
            assignment_args[column_name] = 0
    
    return df.assign(**assignment_args)

# ############################################################
# PUTTING IT ALL TOGETHER ####################################
# ############################################################

def train_and_evaluate():
    result_store = {}

    print('Fetching boosts and saves, project ID: ', PROJECT_ID)
    boosts_with_saves = obtain_boosts_with_saves()
    print('Fetching boosts with redemptions')
    boosts_with_redeems = obtain_boosts_with_prior_redemptions()
    print('Cleaning up and constructing labels')
    data = clean_up_and_construct_labels(boosts_with_saves, boosts_with_redeems)

    print('Value counts for prior redemption: ', data.has_prior_redeemed.value_counts())
    print('Value counts on save within day: ', data.is_save_within_day.value_counts())

    # as below, need to do as type cast
    result_store['n'] = len(data)
    result_store['n_positive'] = int(data.is_save_within_day.value_counts()[True])

    feature_frame = feature_extraction(data)
    print('Data types for feature DF: ', feature_frame.dtypes)

    X_small = feature_frame[["boost_amount_whole_currency", "day_of_month", "day_of_week", "boost_prior_saves", "has_prior_redeemed", "boost_type_category"]]
    # will one hot encode day of week when more data so less sparse
    X_encoded = pd.get_dummies(X_small, prefix_sep="_", columns=["boost_type_category"]) 
    X_encoded = ensure_all_one_hots(X_encoded)
    print('Data types one-hot encoded: ', X_encoded.dtypes)

    X_train, X_test, Y_train, Y_test = train_test_split(X_encoded, data.is_save_within_day, test_size=0.2)
    
    # see results notebook for removing C = 1000 for the moment
    param_grid = [
        {'C': [1, 10, 100], 'kernel': ['linear'], 'class_weight': ['balanced'] },
        {'C': [1, 10, 100], 'gamma': [0.001, 0.0001], 'kernel': ['rbf'], 'class_weight': ['balanced'] },
    ]
    print('Established parameter grid: ', param_grid)

    search_svc = svm.SVC()
    svc_clf = GridSearchCV(search_svc, param_grid, verbose=True, n_jobs=2, cv=2)

    # and here we go
    print('Initiating training')
    svc_clf.fit(X_encoded, data.is_save_within_day)

    # note : these get logged in a datastore; todo : use to decide whether to replace curr model (but then need consistent test data)
    scores = precision_recall_fscore_support(Y_test, svc_clf.predict(X_test), average='binary')
    print('Results: ', scores)
    precision, recall, fscore, support = scores

    # type conversion is for later storage (sklearn returns as numpy.float not plain float, and gcp gets annoyed)
    result_store['precision'] = float(precision)
    result_store['recall'] = float(recall)
    result_store['fscore'] = float(fscore)
    # result_store['support'] = support # sometimes comes out as None, in which case causes bit of unpleasantness
 
    result_store['accuracy'] = accuracy_score(Y_test, svc_clf.predict(X_test))
    print('Accuracy: ', result_store['accuracy'])
    
    return svc_clf, result_store, feature_frame
    