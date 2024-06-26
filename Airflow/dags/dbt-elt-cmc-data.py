import os

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
        Copies the credential file to the shared named volume.
        """
        try:
            shutil.copy(source_path, destination_path)
            print(f"File copied from {source_path} to {destination_path}")
        except Exception as e:
            print(f"Error copying file: {e}")


    @task()
    def dbt_run():
        """
        Executes 'dbt -d run --target prod'
        """

        operator = DockerOperator(
            task_id="dbt_run_task",
            image="justinaslorjus/dbt_cmc_elt:0.1.1",
            trigger_rule="none_failed",
            command=["-d", "run", "--target", "prod"],
            auto_remove=True,
            mount_tmp_dir=False,
            mounts=[
                Mount(source=f"airflow_shared-creds-volume", target="/shared_creds", type="volume"),
            ],
        )

        operator.execute(context=None)
    
    copy_task = copy_file('/opt/airflow/config/shared_creds/gcp_default.json', '/opt/airflow/shared/gcp_default.json')
    dbt_run_task = dbt_run()

    copy_task >> dbt_run_task

dbt_cmc_elt()