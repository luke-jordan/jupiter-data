# Building off of:

# https://towardsdatascience.com/how-to-train-machine-learning-models-in-the-cloud-using-cloud-ml-engine-3f0d935294b3
# https://github.com/GoogleCloudPlatform/cloudml-samples/blob/master/sklearn/sklearn-template/template/setup.py

import setuptools

REQUIRED_PACKAGES = [
    'numpy>=1.19.0',
    'pandas>=1.0.5',
    'scikit-learn>=0.23.1',
    'docopt'
    # 'cloudml-hypertune',
]

setuptools.setup(
    name='jupiter-boost-targeting',
    version='0.0.1',
    author='Luke Jordan',
    author_email='luke@jupitersave.com',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    description='Package to run on Cloud ML Engine to train a model to select users for boost offers'   
)