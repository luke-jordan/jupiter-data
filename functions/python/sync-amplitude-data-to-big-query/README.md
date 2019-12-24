# Sync Amplitdue Data To Big Query
## Overview - How it works

`sync-amplitude-data-to-big-query` serves to retrieve data from Amplitude, process the data and
load it into Big Query.

`sync-amplitude-data-to-big-query` is triggered by a cloud scheduler (`fire-amplitude-to-big-query-sync`) which runs at 3am UTC every day.

The cloud scheduler sends a message to pub/sub topic (`daily-runs`), our function `sync-amplitude-data-to-big-query` has a 
subscription to `daily-runs` and is triggered when the message arrives from `fire-amplitude-to-big-query-sync`.

When `sync-amplitude-data-to-big-query` (now referred to as `the script`) runs it hits the Amplitude API and downloads a gzipped file containing our data for the previous day, the script stores a copy of the downloaded file in Google cloud storage bucket: `gs://staging-sync-amplitude-data-to-big-query-bucket/export` (for production: `gs://production-sync-amplitude-data-to-big-query-bucket/export`). The script then unzips the file and transforms it into a data format suitable for Big Query table: `amplitude.events` and `amplitude.events_properties`. The script then stores a copy of the formatted data in 
Google cloud storage bucket: `gs://staging-sync-amplitude-data-to-big-query-bucket/import` (for production: `gs://production-sync-amplitude-data-to-big-query-bucket/import`). 
The script also loads the formatted data into the Big query table: `ops.all_user_events`

Then the script commences cleanups which involves removing the raw downloaded files and the formatted files stored in the local directory of the script.



# The below was written by Martijn Scheijbeler: https://github.com/martijnsch/amplitude-bigquery and it explains how the script is used

# Amplitude > Google Cloud Storage > Google BigQuery
Export your [Amplitude](https://amplitude.com/) data to [Google BigQuery](https://bigquery.cloud.google.com) for big data analysis.
This script will download all events & properties from the Amplitude
Export API, parse the data and prepare a data job for Google BigQuery
by storing the data for backup purposes in [Google Cloud Storage](https://cloud.google.com/storage/).

Read more about this integration [here](http://www.martijnscheijbeler.com/import-amplitude-into-google-bigquery/) on the blog of [Martijn Scheijbeler](http//www.martijnscheijbeler.com).

[![forthebadge](https://forthebadge.com/images/badges/fuck-it-ship-it.svg)](https://forthebadge.com)


## Features / Support
* Download data for a full day from Amplitude using the Export API
* Parse the data to match data types in Google BigQuery
* Export new parsed files for a load data job in Google BigQuery
* Store backup data in Google Cloud Storage
* Cleans up after use, all temporary files will be deleted.


## Quick start:
1. Clone this repository: `git clone git@github.com:MartijnSch/amplitude-bigquery.git`
2. Fill in your Amplitude Account ID: `ACCOUNT_ID`, you can find this in Amplitude under your account settings: Settings > Projects
3. Fill in your Amplitude API Key & Secret Key, you can find this in Amplitude under your account settings: Settings > Projects
4. [Create a new project on Google Cloud Platform](https://console.cloud.google.com/home/dashboard)
5. Add the Project ID to the script
6. Activate Google Cloud Storage and Google BigQuery + filled in your billing details
7. Create a bucket in Google Cloud storage with two folders: `export` & `import`
8. Load both schemas (`bigquery-schema-events.json` & `bigquery-schema-events-properties.json`) into Google BigQuery to create the tables
9. Adjust the Constant variables in `amplitude-bigquery.py`
10. Run the script via: `python amplitude-bigquery.py`
11. Look at the backup files in Google Cloud Storage and see the data in Google BigQuery


## Maintainer history
  * [Martijn Scheijbeler](https://github.com/martijnsch) (Current)