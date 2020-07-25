#! /bin/bash

BRANCH=$1

echo "Running with branch: ${BRANCH}"

TRAINER_PACKAGE_PATH="$PWD/boost_target_trainer"
MAIN_TRAINER_MODULE="boost_target_trainer.task"

STORAGE_BUCKET="jupiter_models_${BRANCH}"
STORAGE_BUCKET_PATH="gs://${STORAGE_BUCKET}"

JOB_NAME=boost_induce_training_job_$(date -u +%y%m%d_%H%M%S)
JOB_DIR="gs://jupiter_models_${BRANCH}/boost_inducement/"

[[ $BRANCH == "master" ]] && REGION="europe-west1" || REGION="us-east1"
[[ $BRANCH == "master" ]] && PROJECT_ID="jupiter-production-258809" || PROJECT_ID="jupiter-ml-alpha"

echo "Strange: ${BRANCH}, versus: ${PROJECT_ID}"

gcloud ai-platform local train \
        --package-path $TRAINER_PACKAGE_PATH \
        --module-name $MAIN_TRAINER_MODULE \
        -- \
        --project_id ${PROJECT_ID} \
        --storage_bucket ${STORAGE_BUCKET}

# gcloud ai-platform jobs submit training $JOB_NAME \
#     --staging-bucket $STORAGE_BUCKET_PATH \
#     --job-dir $JOB_DIR \
#     --package-path $TRAINER_PACKAGE_PATH \
#     --module-name $MAIN_TRAINER_MODULE \
#     --region $REGION \
#     --runtime-version 2.1 \
#     --python-version 3.7 \
#     -- \
#     --project_id ${PROJECT_ID} \
#     --storage_bucket ${STORAGE_BUCKET}
