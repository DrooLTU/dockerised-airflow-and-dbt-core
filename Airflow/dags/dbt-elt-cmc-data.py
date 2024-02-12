import os

from airflow.models import Variable
from airflow.decorators import dag, task

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

import pendulum
import shutil

START = pendulum.datetime(2024, 2, 8, tz="UTC")
PROJECT_NAME = os.environ.get('COMPOSE_PROJECT_NAME')

@dag(dag_id='dbt-cmc-elt', start_date=START, schedule="@daily", catchup=False)
def dbt_cmc_elt():
    """
    Runs dbt task(s) in a docker container
    """

    @task()
    def copy_file(source_path: str, destination_path: str):
        """
        This has to handle shared creds.
        """
        try:
            shutil.copy(source_path, destination_path)
            print(f"File copied from {source_path} to {destination_path}")
        except Exception as e:
            print(f"Error copying file: {e}")


    @task()
    def dbt_run():
        """
        Executes 'dbt -d run'
        """

        operator = DockerOperator(
            task_id="dbt_run_task",
            image="justinaslorjus/dbt_cmc_elt:0.1",
            trigger_rule="none_failed",
            command=["-d", "run"],
            auto_remove=True,
            mount_tmp_dir=False,
            mounts=[
                Mount(source=f"airflow_shared-creds-volume", target="/shared_creds", type="volume"),
            ],
        )

        operator.execute(context=None)
    
    copy_task = copy_file('/opt/airflow/config/shared_creds/turing-project-m3-s4-1591ea509586.json', '/opt/airflow/shared/gcp_default.json')
    dbt_run_task = dbt_run()

    copy_task >> dbt_run_task

dbt_cmc_elt()