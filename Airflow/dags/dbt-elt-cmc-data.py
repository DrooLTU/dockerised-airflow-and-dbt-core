import os

from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

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
        fetch_data_task = DockerOperator(
        task_id="fetch_data_task",
        image="justinaslorjus/kaggle_fetch_dataset:1.0-3.11",
        trigger_rule="none_failed",
        command=[
        ],
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source=VOLUME_NAME, target="/data", type="volume"),
        ],
    )
        return
    
    return

dbt_cmc_elt()