#! /bin/bash

TRAINER_PACKAGE_PATH="$PWD/boost_target_trainer"
MAIN_TRAINER_MODULE="boost_target_trainer.task"
# STORAGE_BUCKET="gs://jupiter_models_${CIRCLE_BRANCH}"
STORAGE_BUCKET="gs://jupiter_models_staging"

JOB_NAME=boost_induce_training_job_$(date -u +%y%m%d_%H%M%S)
JOB_DIR="gs://jupiter_models_staging/boost_inducement/"
REGION="us-east1"
# [[ $CIRCLE_BRANCH = "master" ]] && REGION=$MASTER_GOOGLE_COMPUTE_ZONE || REGION=$STAGING_GOOGLE_COMPUTE_ZONE

gcloud ai-platform local train \
        --package-path $TRAINER_PACKAGE_PATH \
        --module-name $MAIN_TRAINER_MODULE \
        -- \
        --project-id "jupiter-ml-alpha"

# gcloud ai-platform jobs submit training $JOB_NAME \
#     --staging-bucket $STORAGE_BUCKET \
#     --job-dir $JOB_DIR \
#     --package-path $TRAINER_PACKAGE_PATH \
#     --module-name $MAIN_TRAINER_MODULE \
#     --region $REGION \
#     --runtime-version 2.1 \
#     --python-version 3.7 \
#     -- \
#     --project-id "jupiter-ml-alpha"
