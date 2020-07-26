# Makes a prediction, after enrichment, using model

import pandas as pd

from google.cloud import bigquery as bq

bq_client = bq.Client()

def enrich_users_from_bq(user_ids):
    # we need to get their (i) prior save count, (ii) when first saved, (iii) last saved, and (iv) if they have redeemed prior
    # the first can be found from the user behaviour table, and the second from the all events table
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter("user_ids", "STRING", user_ids)
        ]
    )

    save_query = """
        with save_data as (select user_id, 
            count(*) as prior_save_count,
            max(TIMESTAMP_MILLIS(time_transaction_occurred)) as latest_save_date,
            min(TIMESTAMP_MILLIS(time_transaction_occurred)) as earliest_save_date
        from ops.user_behaviour
            where transaction_type = 'SAVING_EVENT'
            and user_id in UNNEST(@user_ids) group by user_id) 

        select user_id, prior_save_count, 
            latest_save_date, TIMESTAMP_DIFF(current_timestamp, latest_save_date, HOUR) as hours_since_latest, 
            earliest_save_date, TIMESTAMP_DIFF(current_timestamp, earliest_save_date, HOUR) as hours_since_earliest, 
        from save_data
    """
    
    df = bq_client.query(save_query, job_config=job_config).to_dataframe()

    boost_query = """
    select user_id, count(*) as number_boost_redeems 
    from ops.all_user_events where event_type = 'BOOST_REDEEMED' and user_id in UNNEST(@user_ids)
    group by user_id, event_type
    """

    boost_df = bq_client.query(boost_query, job_config=job_config).to_dataframe()

    df = df.merge(boost_df, how='left').fillna(0)

    return df

def add_one_hots(df, boost_type_category):
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
        'WITHDRAWAL::CANCEL_WITHDRAWAL'
    ]
    
    assignment_args = {}
    for category in all_boost_type_categories:
        column_name = f'boost_type_category_{category}'
        assignment_args[column_name] = int(category == boost_type_category)
    
    return df.assign(**assignment_args)

def make_inference(user_ids, boost, model):
    # for these users, get a dataframe from bq with some necessary data
    bq_df = enrich_users_from_bq(user_ids)
    if len(bq_df) == 0:
        # sometimes we get asked about users with no prior information (or for whome queries come back empty)
        return pd.DataFrame({ 'user_id': user_ids, 'should_offer': False })

    df = bq_df[['user_id']]
    df = df.assign(
        boost_amount_whole_currency=boost["boost_amount_whole_currency"],
        days_since_latest_save=bq_df["hours_since_latest"] / 24,
        days_since_first_save=bq_df["hours_since_earliest"] / 24,
        boost_prior_saves=bq_df["prior_save_count"],
        has_prior_redeemed=bq_df['number_boost_redeems'] > 0,
        day_of_week=pd.to_datetime('today').dayofweek,
        day_of_month=pd.to_datetime('today').day
    )

    # until more data, avoiding excessively sparse feature space (but review soon)
    X_small = df[["boost_amount_whole_currency", "day_of_month", "day_of_week", "boost_prior_saves", "has_prior_redeemed"]]
    print('Assembled feature dict: ', X_small.head())

    X_encoded = add_one_hots(X_small, boost["boost_type_category"])
    print('Assembled and encoded, proceeding to predict')

    predictions = model.predict(X_encoded)
    print('Made predictions: ', predictions)

    df["should_offer"] = predictions
    return df
