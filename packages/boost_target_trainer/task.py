"""Run a training job on Cloud ML Engine for boost inducement.
Usage:
  boost_trainer.task --project-id <project_id>  [--max_c <max_c>]

Options:
  -h --help     Show this screen.
  --project-id <project-id> The project ID (for the dataset locations, etc)
  --max-c <max-c>  SVM regularization (bit of a dummy for now) [default: 100]
"""
from docopt import docopt

from boost_target_trainer import model
from boost_target_trainer import util

def main(arguments=None):
    # model.OUTPUT_DIR = arguments['<outdir>']
    # model.BUCKET_NAME = arguments['<bucket_name>']
    print('Executing, received arugments: ', arguments)
    model.PROJECT_ID = arguments['--project-id']

    print('Executing, project ID set to: ', model.PROJECT_ID)
    clf, results, dataframe = model.train_and_evaluate()
    util.persist_model(clf)

if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
