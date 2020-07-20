"""Run a training job on Cloud ML Engine for boost inducement.
Usage:
  boost_trainer.task --project_id <project_id>  [--max_c <max_c>]

Options:
  -h --help     Show this screen.
  --project_id <project_id> The project ID (for the dataset locations, etc)
  --storage_bucket <storage_bucket> The bucket where to place the model once completed
  --max-c <max-c>  SVM regularization (bit of a dummy for now) [default: 100]
"""
# from docopt import docopt
import argparse

from boost_target_trainer import model
from boost_target_trainer import util

def main(arguments=None):
    # model.OUTPUT_DIR = arguments['<outdir>']
    # model.BUCKET_NAME = arguments['<bucket_name>']
    print('Executing, received arguments: ', arguments)
    model.PROJECT_ID = arguments['--project_id']

    print('Executing, project ID set to: ', model.PROJECT_ID)
    clf, results, dataframe = model.train_and_evaluate()
    
    print('Now persisting internally (phase out eventually for ML engine')
    util.BUCKET_NAME = arguments['--storage_bucket']
    util.persist_model(clf)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # using this for now instead of docopt because that is being badly fragile
    parser.add_argument(
      '--project_id',
      help='Project ID',
      default='jupiter-ml-alpha'
    )
    parser.add_argument(
      '--storage_bucket',
      help='Bucket where to place model when done',
      default='jupiter_models_staging'
    )
    args, unknown = parser.parse_known_args()
    arguments = { '--project_id': args.project_id, '--storage_bucket': args.storage_bucket }
    main(arguments)
