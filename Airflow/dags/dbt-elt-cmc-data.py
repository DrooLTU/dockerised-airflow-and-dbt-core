import os

from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task

import pendulum

START = pendulum.datetime(2024, 2, 8, tz="UTC")

@dag(dag_id='dbt-cmc-elt', start_date=START, schedule="@daily", catchup=False)
def dbt_cmc_elt():
    """
    Runs dbt task(s) in a docker container
    """

    @task()
    def dbt_run():
        """
        Executes 'dbt run'
        """

        return
    
    return

dbt_cmc_elt()