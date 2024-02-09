import os

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


from typing import Any, List
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pendulum
import pandas as pd


now = pendulum.now()
SYMBOL = 'BTC'

@dag(start_date=now, schedule_interval='*/10 * * * *', catchup=False)
def cmc_api_etl():

    @task(depends_on_past=False)
    def retrieve() -> str:
        """
        Fetch data from CMC API.
        """

        URL = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
        API_KEY = Variable.get("CMC_API_KEY")
        PARAMS = {
        'symbol':SYMBOL,
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
    def json_to_df(path: str) -> pd.DataFrame:
        """
        Reads a JSON file and flattens nested data.
        Returns a DataFrame

        Args:
        path: path to JSON file.
        """

        if path.endswith(".json"):
            with open(path, "r") as json_str:
                data = json.load(json_str)
                flattened_data = pd.json_normalize(data['data'], SYMBOL, sep='__')
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
        """
        file_path = os.path.join(path, file_name)
        df.to_csv(file_path, index=False)

        return file_path


    @task(depends_on_past=False)
    def dataframe_to_parquet(df: pd.DataFrame, path: str, file_name: str) -> str:
        """
        Converts pd.DataFrame to parquet.
        """
        file_path = os.path.join(path, file_name)
        df.to_parquet(file_path) 
        return file_path


    @task(depends_on_past=False)
    def local_to_gcs(file_path: str, dist_file_name: str, bucket: str) -> str:
        """
        Loads local file into GCS bucket.
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
    PARQUET_FILE_NAME = 'flattened_api_data_BTC.parquet'
    data = retrieve()
    local_json_path = raw_to_local('raw_data.json', data, PATH)
    df = json_to_df(local_json_path)
    csv = dataframe_to_csv(df, PATH, 'flattened_api_data_BTC.csv')
    parquet = dataframe_to_parquet(df, PATH, PARQUET_FILE_NAME)
    dst_path = local_to_gcs(parquet, PARQUET_FILE_NAME, 'm3s4-standard-data-storage')
    gcs_to_bq(dst_path, 'raw')


cmc_api_etl()

