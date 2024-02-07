from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.providers.google.transfers.transfers import BigQueryInsertJobOperator, PandasDataFrameToCsvFileOperator



from datetime import datetime, timedelta
from typing import Any, List
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pendulum
import pandas as pd


now = pendulum.now()

@dag(start_date=now, schedule="@hourly", catchup=False)
def etl():
    @task()
    def retrieve() -> Any:
        """
        Fetch data from CMC API.
        """

        URL = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
        API_KEY = Variable.get("CMC_API_KEY")
        PARAMS = {
        'symbol':'BTC',
        }

        HEADERS = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY,
        }

        session = Session()
        session.headers.update(HEADERS)

        try:
            response = session.get(URL, params=PARAMS)
            data = json.loads(response.text)
            return data
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            AirflowException(e)


    @task()
    def to_df(data: Any) -> pd.DataFrame:
        """WIP
        Convert nested JSON to flat DataFrame.
        """
        flattened_data = pd.json_normalize(data['data'])
        print(flattened_data)
        return flattened_data


    @task()
    def dataframe_to_csv(df: pd.DataFrame, path: str) -> str:
        """
        Write DataFrame to CSV.
        """
        df.to_csv(path, index=False)
        return path


    @task()
    def load_to_bq(csv_file_path: str) -> Any:
        """
        WIP
        Load data to BigQuery 'raw' table.
        """

        configuration = {
            'load': {
                'sourceFormat': 'CSV',
                'skip_leading_rows': 1,
                'destinationTable': {
                    'projectId': 'your_project_id',
                    'datasetId': 'your_dataset_id',
                    'tableId': 'your_table_id',
                },
            },
        }

        operator = BigQueryInsertJobOperator(
            task_id='load_csv_to_bigquery',
            configuration=configuration,
            source_uris=[csv_file_path],
        )
        return operator.execute(context=None)

    PATH = './data/flattened_api_data.csv'

    data = retrieve()
    df = to_df(data)
    csv = dataframe_to_csv(df, PATH)
    load_to_bq(csv)


etl()

