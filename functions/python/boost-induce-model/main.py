# Needed because GCF requires entry point to be in main.py

from .train import train_model
from .infer import make_inference

def train_boost_inducement_model():
    return 'hello world'

def infer_boost_inducement():
    return ['user-id']
    