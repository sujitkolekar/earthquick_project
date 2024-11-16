## importing the libraries and module required

from pyspark.sql import SparkSession
import configuration as conf
from utility import ReadDataFromApiJson,UploadtoGCS,GCSFileDownload,EarthquakeDataFrameCreation,Transformation,SilverParquet,UploadToBigquery


### creating spark session

# spark = SparkSession.builder.appName('earthquke-project').getOrCreate()





def main():
    ## read data from API
    content = ReadDataFromApiJson.reading(conf.URL_MONTH)
    ## upload data to GCS bucket landing layer
    upload = UploadtoGCS.uploadjson(conf.BUCKET_NAME,content)
    ## read data from GCS bucket
    json_data = GCSFileDownload(conf.BUCKET_NAME).download_json_as_text(conf.READ_JSON_FROM_CLOUD)
    ## Fatten and create dataframe
    dataframe = EarthquakeDataFrameCreation(json_data).convert_to_dataframe()
    ## transformation on data frame
    df = Transformation.process(dataframe)
    # upload to parqute in silver layer
    parquate_upload = SilverParquet.upload_parquet(df,conf.WRITE_PARQUATE)
    ## write to bq
    bq_upload = UploadToBigquery(conf.PROJECT_ID,conf.DATASET_NAME,conf.STAGING_BUCKET).to_bigquery(conf.TABLE_NAME,df)



if __name__ == '__main__':
    main()

#gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar