# Needed because GCF requires entry point to be in main.py

import os
import json

import record

from train import train_model
from infer import make_inference

local_folder = os.getenv('MODEL_LOCAL_FOLDER', '/tmp')
model_file_prefix = os.getenv('MODEL_FILE_PREFIX', 'boost_inducement_model')
storage_bucket = os.getenv('MODEL_STORAGE_BUCKET', 'jupiter_models_master')

trained_model = record.retrieve_and_load_model()

def select_users_for_boost(request):
    print('Selecting user for boost, received: ', request)
    
    params = request.json
    candidate_users = params['candidate_users']
    boost_parameters = params['boost_parameters']

    prediction_result = make_inference(candidate_users, boost_parameters, trained_model)

    return prediction_result[['user_id', 'should_offer']].to_json(orient='records')


def train_boost_inducement_model(event=None, context=None):
    local_folder = os.getenv('MODEL_LOCAL_FOLDER')
    model_file_prefix = os.getenv('MODEL_FILE_PREFIX', 'boost_inducement_model')
    storage_bucket = os.getenv('MODEL_STORAGE_BUCKET')

    print(f'''
        Initiating boost inducement model training, with local folder: {local_folder}, 
        model_prefix: {model_file_prefix}, and storage_bucket={storage_bucket}
    ''')
    
    # clf, results, dataframe = train_model(local_folder=local_folder, model_file_prefix=model_file_prefix, storage_bucket=storage_bucket)
    # print('Completed training (and persisting) model, now to record it')

    # print('Persisting model, possibly to storage')
    # record.persist_model(clf, local_folder, model_file_prefix, storage_bucket, True)

    # print('Also persisting dataframe')
    # record.store_and_send_results(results)

    # trained_model = clf

    return 'OK'

# if __name__ == '__main__':
#     train_boost_inducement_model()
