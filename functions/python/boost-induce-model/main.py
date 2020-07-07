# Needed because GCF requires entry point to be in main.py

import os

from train import train_model
# from .infer import make_inference

def train_boost_inducement_model():
    local_folder = os.getenv('MODEL_LOCAL_FOLDER')
    model_file_prefix = os.getenv('MODEL_FILE_PREFIX', 'boost_inducement_model')
    storage_bucket = os.getenv('MODEL_STORAGE_BUCKET')

    print(f'''
        Initiating boost inducement model training, with local folder: {local_folder}, 
        model_prefix: {model_file_prefix}, and storage_bucket={storage_bucket}
    ''')
    train_model(local_folder=local_folder, model_file_prefix=model_file_prefix, storage_bucket=storage_bucket)
    
    print('Completed training (and persisting) model')

def infer_boost_inducement():
    return ['user-id']

if __name__ == '__main__':
    train_boost_inducement_model()
