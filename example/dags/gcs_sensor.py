"""
This example shows how to use the trigger_dag_factory with the 
"GcsMovefilesTriggerDagConfigurationSingle" configuration, which listens for 
files arriving in a bucket.

Once a file has arrived, it derives the execution date, for the DAG to trigger,
by evaluating the suffix of the file that arrived.
"""

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from unaflow.dags.trigger_dag.providers.google.gcs_context_functions import evaluate_execution_date_on_file_suffix

from unaflow.dags.trigger_dag.trigger_dag_factory import trigger_dag_factory
from unaflow.dags.trigger_dag.providers.google.gcs_trigger_dag_configuration import GcsMovefilesTriggerDagConfigurationSingle

# A simple "Hello world" DAG
with DAG(
        dag_id="example_dag",
        schedule_interval=None,  # Important this be None. Or else, the trigger is not responsible.
        start_date=datetime(2021, 1, 1)
) as hello_world:
    echo = PythonOperator(
        task_id="echo",
        python_callable=lambda **kwargs: print(kwargs['dag_run'].conf)
    )


class GcsTriggerOnFilename(GcsMovefilesTriggerDagConfigurationSingle):
    def execution_date(*args, **kwargs):
        return evaluate_execution_date_on_file_suffix(kwargs['ti'])

    def create_downstream_triggering(self):
        return BashOperator(
            task_id="the_end",
            bash_command="echo the end"
        )

    # Override the task for finding the top file,
    # and add another task. This could for example
    # be a task that adds some metadata to the
    # folder that is being moved around.
    def create_find_top_file_task(self):
        top_file = super().create_find_top_file_task()
        meta_data = BashOperator(
            task_id="meta_data",
            bash_command=f"echo creating meta data for {top_file.task_id}"
        )

        top_file >> meta_data

        return [top_file, meta_data]

    # Add a task that sends a slack message
    # after the sensor has succesfully received a message
    def create_downstream_sensor(self):
        return [
            super().create_downstream_sensor(),
            BashOperator(
                task_id="slack_message",
                bash_command="echo slack this"
            )]


# This is the trigger DAG, that triggers the "Hello world" DAG
dag = trigger_dag_factory(
    config=GcsTriggerOnFilename(
        trigger_dag_id=hello_world.dag_id,
        bucket="{{ var.value.gcs_sensor_bucket }}",
        prefix="file_",
        destination="in-progress",
        clear_dag=True,
    ),
    start_date=hello_world.start_date,
)
