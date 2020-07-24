# Needed because GCF requires entry point to be in main.py

import os
import json

import record

from train import train_model
from infer import make_inference

local_folder = os.getenv('MODEL_LOCAL_FOLDER', '/tmp')
model_file_prefix = os.getenv('MODEL_FILE_PREFIX', 'boost_target_model_latest')
storage_bucket = os.getenv('MODEL_STORAGE_BUCKET', 'jupiter_models_master')

def select_users_for_boost(request):
    print('Selecting user for boost, received: ', request)

    # for the moment, doing this in here, because it's relatively quick (few millis) and GCP seems
    # to have a different container spin-up method than AWS (i.e., need a new deployment to refresh)
    print('Fetch model (should be quick)')
    trained_model = record.retrieve_and_load_model()
    
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
    
    # This has become far too heavy for cloud function already, so moved across to ML Engine
    # May use this to, via ML engine, trigger a new job again
    print('Would have trained model, for now, just exiting')

    return 'OK'

# if __name__ == '__main__':
#     train_boost_inducement_model()
