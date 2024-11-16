import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions,StandardOptions
import os
import requests
import json
from datetime import datetime
from apache_beam.io.parquetio import WriteToParquet
import pyarrow as pa
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'C:\Users\yogit\PycharmProjects\Earthquick_project_restart\white-script-441216-k7-20080f301d0e.json'

    options = PipelineOptions()

    # Specify Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'white-script-441216-k7'
    google_cloud_options.job_name = 'silverlayer1'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = 'gs://earthquick_project/dataflow/staging_location/'
    google_cloud_options.temp_location = 'gs://earthquick_project/dataflow/temp_location/'

    options.view_as(StandardOptions).runner = 'DataflowRunner'






########################################################################################################################
class  fetch_data_from_api(beam.DoFn):
    def process(self,api_url):
        import requests,json
        """
           Fetches data from the given API URL using the requests library.

           Args:
           url (str): The API endpoint URL.

           Returns:
           dict: The JSON response data if successful, or None if the request fails.
           """
        response=requests.get(api_url)
        if response.status_code==200:
            data=response.json()
            yield data
        else:
            raise Exception (f'Failed to fetch_data status_code:{response.status_code}')


#################################################################################################################

    #
    # def upload_api_data_to_gcs(bucket_name,destination_file_name,data):
    #
    #     """
    #     Write the API data to GCS as a JSON file using GCS Client Libraries.
    #
    #     :param data: Data fetched from the API (Python dictionary).
    #     :param bucket_name: Name of the GCS bucket.
    #     :param file_name: File name to save the data as in the GCS bucket.
    #     """
    #
    #     # initializing a gcs client
    #
    #     storage_client = storage.Client()
    #
    #     bucket = storage_client.bucket(bucket_name)
    #
    #     blob = bucket.blob(destination_file_name)
    #
    #     blob.upload_from_string(json.dumps(data), content_type='application/json')
    #
    #     print(f'data uploaded to {bucket_name}/{destination_file_name}')


########################################################################################################################
class AddColumnArea(beam.DoFn):
    """
    Add a column 'area' to the earthquake data based on the 'place'.
    """

    def process(self, record):
        place = record["place"]
        if "of" in place:
            area = place.split("of")[1].strip()  # Safely extract the area after 'of'
        else:
            area = "Unknown"  # Handle cases where 'of' is not found
        record["area"] = area
        yield record


    ########################################################################################################################




class AddInsertDate(beam.DoFn):

    """
    Add an insert_date column to each record with the current date in 'YYYY-MM-DD' format.
    """

    def process(self, record):
        from datetime import datetime
        record["insert_date"] = datetime.utcnow().strftime('%Y-%m-%d')
        yield record


class FlattenJSONData(beam.DoFn):
    """
    Flatten the JSON structure to prepare it for writing to BigQuery or other outputs.

    """

    def convert_timestamp_to_gmt(self,timestamp_ms):
        from datetime import datetime
        """
        Convert Unix timestamp in milliseconds to GMT.
        """
        if timestamp_ms is not None:
            timestamp_s = timestamp_ms / 1000
            return datetime.utcfromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
        return None



    def process(self, json_data):
        features = json_data.get("features", [])
        for feature in features:
            properties = feature["properties"]
            geometry = feature["geometry"]
            coordinates = geometry["coordinates"]

            flattened_record = {
                "place": str(properties.get("place")),
                "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
                "time": str(self.convert_timestamp_to_gmt((properties.get("time")))),
                "updated_time": str(self.convert_timestamp_to_gmt((properties.get("updated")))),
                "tz": int(properties.get("tz")) if properties.get("tz") is not None else None,
                # Ensure this is an integer
                "url": properties.get("url"),
                "detail": properties.get("detail"),
                "felt": int(properties.get("felt")) if properties.get("felt") is not None else None,
                "cdi": float(properties.get("cdi")) if properties.get("cdi") is not None else None,
                "mmi": float(properties.get("mmi")) if properties.get("mmi") is not None else None,
                "alert": properties.get("alert"),
                "status": properties.get("status"),
                "tsunami": int(properties.get("tsunami")) if properties.get("tsunami") is not None else None,                    "sig": int(properties.get("sig")) if properties.get("sig") is not None else None,
                "net": properties.get("net"),
                "code": properties.get("code"),
                "ids": properties.get("ids"),
                "sources": properties.get("sources"),
                "types": properties.get("types"),
                "nst": int(properties.get("nst")) if properties.get("nst") is not None else None,
                "dmin": float(properties.get("dmin")) if properties.get("dmin") is not None else None,
                "rms": float(properties.get("rms")) if properties.get("rms") is not None else None,
                "gap": float(properties.get("gap")) if properties.get("gap") is not None else None,
                "magType": properties.get("magType"),
                "type": properties.get("type"),
                "title": properties.get("title"),
                "geometry": {
                    "longitude": coordinates[0],
                    "latitude": coordinates[1],
                    "depth": float(coordinates[2]) if coordinates[2] is not None else None
                }
            }
            yield flattened_record


# Define the Parquet schema based on your JSON data structure
parquet_schema = pa.schema([
    ('mag', pa.float32()),
    ('place', pa.string()),
    ('time', pa.string()),
    ('updated_time', pa.string()),
    ('tz', pa.int64()),
    ('url', pa.string()),
    ('detail', pa.string()),
    ('felt', pa.int64()),
    ('cdi', pa.float32()),
    ('mmi', pa.float32()),
    ('alert', pa.string()),
    ('status', pa.string()),
    ('tsunami', pa.int64()),
    ('sig', pa.int64()),
    ('net', pa.string()),
    ('code', pa.string()),
    ('ids', pa.string()),
    ('sources', pa.string()),
    ('types', pa.string()),
    ('nst', pa.int64()),
    ('dmin', pa.float32()),
    ('rms', pa.float32()),
    ('gap', pa.float32()),
    ('magType', pa.string()),
    ('type', pa.string()),
    ("geometry", pa.struct([
        ("longitude", pa.float32()),
        ("latitude", pa.float32()),
        ("depth", pa.float32())
    ])),
    ("area", pa.string())
        # ('insert_date', pa.string())  # Timestamp in seconds
])

    # Define the table schema before it's used
table_schema = {
    "fields": [
        {"name": "place", "type": "STRING", "mode": "NULLABLE"},
        {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "time", "type": "STRING", "mode": "NULLABLE"},
        {"name": "updated_time", "type": "STRING", "mode": "NULLABLE"},
        {"name": "tz", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
        {"name": "felt", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "cdi", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "mmi", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "net", "type": "STRING", "mode": "NULLABLE"},
        {"name": "code", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
        {"name": "types", "type": "STRING", "mode": "NULLABLE"},
        {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
        {"name": "type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "geometry", "type": "RECORD", "mode": "NULLABLE", "fields": [
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"}
        ]},
        {"name": "area", "type": "STRING", "mode": "NULLABLE"},
        {"name": "insert_date", "type": "STRING", "mode": "NULLABLE"}
    ]
}





########################################################################################################################
# Define API URL and GCS bucket
api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
bucket_name = "earthquick_project"
current_date = datetime.now().strftime('%Y-%m-%d')

bronze_output_path = f"gs://earthquick_project/dataflow/bronze-layer/{current_date}"
silver_output_path = f"gs://{bucket_name}/dataflow/silver_layer/{current_date}.json"
parquet_output_path = f"gs://earthquick_project/dataflow/silver_layer/{current_date}"  # .parquet

table_spec = 'white-script-441216-k7:earthquick_ingestion.earthquick_project_dataflow'


def read_api_load_gcs():
    with beam.Pipeline(options=options) as p:
            _ = (
                    p
                    | "read_from_api" >> beam.Create([api_url])
                    | 'read_from_api1' >> beam.ParDo(fetch_data_from_api())
                    | "Format To JSON" >> beam.Map(lambda x: json.dumps(x))
                    | "Write Raw Data to GCS" >> beam.io.WriteToText(bronze_output_path,
                                                                     file_name_suffix=".json",
                                                                     num_shards=1
                                                                     )
            )


def read_gcs_transform():
    with beam.Pipeline(options=options) as p:
            _ = (
                    p
                    | "read_data_from_gcs" >> beam.io.ReadFromText(bronze_output_path + '*')
                    | "load_json_data" >> beam.Map(json.loads)
                    | "flatten_the_data" >> beam.ParDo(FlattenJSONData())
                    | "Add Column Area" >> beam.ParDo(AddColumnArea())
                    | 'Write to Parquet' >> WriteToParquet(
                file_path_prefix=parquet_output_path,
                schema=parquet_schema,
                file_name_suffix=".parquet")
            )


def load_to_bigquery():
    with beam.Pipeline(options=options) as p:
            _ = (
                    p
                    | "read_from_Silver_layer" >> beam.io.ReadFromParquet(parquet_output_path + '*.parquet')
                    | 'add_insert_date_column' >> beam.ParDo(AddInsertDate())
                    | "Write_Data_to_Bigquery" >> beam.io.WriteToBigQuery(table=table_spec,
                                                                          schema=table_schema,
                                                                          write_disposition=BigQueryDisposition.WRITE_APPEND,
                                                                          create_disposition=BigQueryDisposition.CREATE_IF_NEEDED)

            )

try:
    read_api_load_gcs()
    read_gcs_transform()
    load_to_bigquery()
except Exception as e :
    print(f"Error occured while running pipeline {e}")



