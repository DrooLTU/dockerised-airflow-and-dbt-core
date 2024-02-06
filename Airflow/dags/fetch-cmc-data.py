from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task


from datetime import datetime, timedelta
from typing import List
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pendulum


now = pendulum.now()

@dag(start_date=now, schedule="@hourly", catchup=False)
def etl():
    @task()
    def retrieve() -> dict:
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
            print(data)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            AirflowException(e)


    @task()
    def to_df():
        """WIP
        Convert nested JSON to flat dataframe.
        """
        df = pd.to_df()
        return df

    @task()
    def load(df):
        """
        WIP
        Load data to BigQuery 'raw' table.
        """

        return 

    data = retrieve()
    df = to_df(data)
    load(df)


etl()

