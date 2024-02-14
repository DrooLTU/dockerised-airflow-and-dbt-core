import os

from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from typing import Any
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pendulum
import pandas as pd


START = pendulum.datetime(2024, 2, 8, tz="UTC")
SYMBOLS = ['BTC', 'ETH']

for symbol in SYMBOLS:
    dag_id = f'cmc_price_{symbol}'
    @dag(dag_id=dag_id, start_date=START, schedule_interval='*/10 * * * *', catchup=False)
    def cmc_api_etl():

        @task(depends_on_past=False)
        def retrieve(symbol: str) -> str:
            """
            Fetch data from CMC API.
            """

            URL = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
            API_KEY = Variable.get("CMC_API_KEY")
            PARAMS = {
            'symbol':symbol,
            }

            HEADERS = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': API_KEY,
            }

            session = Session()
            session.headers.update(HEADERS)

            try:
                response = session.get(URL, params=PARAMS)
                data = response.text
                return data
            except (ConnectionError, Timeout, TooManyRedirects) as e:
                raise AirflowException(e)


        @task(depends_on_past=False)
        def raw_to_local(filename: str, file_content: str, dir: str):
            """
            Saves a file to the specified directory.

            Args:
            filename: The name of the file to save.
            file_content: The content of the file to save.
            dir: The directory to save the file to.
            """

            if not os.path.exists(dir):
                os.makedirs(dir)

            file_path = os.path.join(dir, filename)

            with open(file_path, "w") as f:
                f.write(file_content)

            return file_path


        @task(depends_on_past=False)
        def json_to_df(path: str, symbol:str) -> pd.DataFrame:
            """
            Reads a JSON file and flattens nested data.
            Returns a DataFrame

            Args:
            path: path to JSON file.
            symbol: coin symbol, eg: 'ETH'
            """

            if path.endswith(".json"):
                with open(path, "r") as json_str:
                    data = json.load(json_str)
                    flattened_data = pd.json_normalize(data['data'], symbol, sep='__')
                    columns_to_convert = ['circulating_supply', 'total_supply']
                    flattened_data[columns_to_convert] = flattened_data[columns_to_convert].astype(int)
                    print(flattened_data)

                    return flattened_data

            else:
                raise AirflowException(
                    f"Unsupported file format, expected JSON, got {path.split('.')[-1]}"
                )
            

        @task(depends_on_past=False)
        def dataframe_to_csv(df: pd.DataFrame, path: str, file_name: str) -> str:
            """
            Writes pd.DataFrame to CSV.

            Args:
            df: Pandas DataFrame.
            path: path to save location.
            file_name: name for the saved file.
            """
            file_path = os.path.join(path, file_name)
            df.to_csv(file_path, index=False)

            return file_path


        @task(depends_on_past=False)
        def dataframe_to_parquet(df: pd.DataFrame, path: str, file_name: str) -> str:
            """
            Converts pd.DataFrame to parquet.

            Args:
            df: Pandas DataFrame.
            path: path to save location.
            file_name: name for the saved file.
            """
            file_path = os.path.join(path, file_name)
            df.to_parquet(file_path) 
            return file_path


        @task(depends_on_past=False)
        def local_to_gcs(file_path: str, dist_file_name: str, bucket: str) -> str:
            """
            Loads local file into GCS bucket.

            Args:
            file_path: full path to the file to be uploaded.
            dist_file_name: name for the file to be asve as in the bucket.
            bucket: bucket id to upload to.
            """
            dst_path = f'data/{dist_file_name}'

            operator = LocalFilesystemToGCSOperator(
                task_id='local_to_gcs',
                bucket=bucket,
                src=file_path,
                dst= dst_path
            )

            operator.execute(context=None)

            return dst_path


        @task(depends_on_past=False)
        def gcs_to_bq(dst_path: str, table_id: str, **kwargs) -> Any:
            """
            Load data to BigQuery table.

            Args:
            dst_path: path to the file in the storage bucket.
            table_id: id of the table to append data to.
            """

            context = kwargs.copy()
            context.update({
                "logical_date": pendulum.now(),
            })
        
            operator = GCSToBigQueryOperator(
                task_id='load_to_bq',
                bucket='m3s4-standard-data-storage',
                source_objects=[dst_path],
                destination_project_dataset_table=f'{Variable.get("gcp_default_project_id")}.{Variable.get("bq_raw_dataset")}.{table_id}',
                source_format='Parquet',
                write_disposition="WRITE_APPEND",
            )

            return operator.execute(context=context)


        PATH = './data/'
        PARQUET_FILE_NAME = f'flattened_api_data_{symbol}.parquet'
        data = retrieve(symbol)
        local_json_path = raw_to_local(f'raw_data_{symbol}.json', data, PATH)
        df = json_to_df(local_json_path, symbol)
        csv = dataframe_to_csv(df, PATH, f'flattened_api_data_{symbol}.csv')
        parquet = dataframe_to_parquet(df, PATH, PARQUET_FILE_NAME)
        dst_path = local_to_gcs(parquet, PARQUET_FILE_NAME, 'm3s4-standard-data-storage')
        gcs_to_bq(dst_path, 'raw')


    cmc_api_etl()

