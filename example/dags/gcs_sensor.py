import re
from datetime import datetime
from airflow.utils import timezone

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from unaflow.dags.google_cloud_storage_sensor_dag import GoogleCloudStoragePrefixSensorDAG

# A simple "Hello world" DAG
with DAG(
        dag_id="example_dag",
        schedule_interval=None,  # Important this be None
        start_date=datetime(2021, 1, 1)
) as hello_world:
    echo = BashOperator(
        task_id="echo",
        bash_command="echo hello"
    )


def execution_date_from_filename(dag: GoogleCloudStoragePrefixSensorDAG,
                                 find_first=GoogleCloudStoragePrefixSensorDAG.find_first,
                                 **context):
    """
    We expect filenames to be on the format "file_20201212"
    Meaning, we parse the last 8 chars to a date.

    :param dag: The GoogleCloudStoragePrefixSensorDAG, from which to get the sensors files from
    :param find_first: The function for getting the xcom result from the sensor
    :param context: context of dag_run to send to the find_first function
    :return: execution_date from the filename
    """

    filename = find_first(dag.sensor.task_id, **context)
    pattern = re.compile('.*_([0-9]{8})')
    match = pattern.match(filename)
    return str(timezone.parse(match.group(1), timezone=timezone.utc))


# This is the trigger DAG, that triggers the "Hello world" DAG
sensor_dag = GoogleCloudStoragePrefixSensorDAG(
    trigger_dag_id=hello_world.dag_id,
    bucket="{{ var.value.gcs_sensor_bucket }}",
    prefix="file_",
    destination="in-progress",
    trigger_dag_execution_callable=execution_date_from_filename,
    start_date=hello_world.start_date
)
