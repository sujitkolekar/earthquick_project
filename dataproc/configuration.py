## importing the libraries and module required

from datetime import datetime


## google service account key
GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\yogit\PycharmProjects\Earthquick_project_restart\compute_service_key\white-script-441216-k7-202da19c80a9.json'

## url for monthly basis


URL_MONTH = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

## url for daily basis

URL_DAILY = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'



### GCP storage Bucket variables
PROJECT_NAME = 'My First Project'



BUCKET_NAME = 'earthquick_by_dataproc'



### FILE PATHS TO READ AND WRITE



# Create a unique file name with the current date
date_str = datetime.now().strftime('%Y%m%d')


READ_JSON_FROM_CLOUD = f"landing_layer/{date_str}.json"
READ_JSON_FROM_CLOUD_DAILY = f"landing_layer/daily{date_str}.json"


# for silver layer path


WRITE_PARQUATE = f'gs://{BUCKET_NAME}/silver_layer/{date_str}.parquet'

WRITE_PARQUATE_DAILY = f'gs://{BUCKET_NAME}/silver_layer/daily{date_str}.parquet'

## bigquary variables

PROJECT_ID = 'white-script-441216-k7'

DATASET_NAME = 'earthquick_ingestion'

TABLE_NAME = 'earthquick_project_dataproc'

STAGING_BUCKET = 'earthquake-project-staging-bucket'