"""
This example shows how to use the trigger_dag_factory with the 
"GcsMovefilesTriggerDagConfigurationSingle" configuration, which listens for 
files arriving in a bucket.

Once a file has arrived, it derives the execution date, for the DAG to trigger,
by evaluating the suffix of the file that arrived.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from unaflow.dags.trigger_dag.providers.google.gcs_context_functions import evaluate_execution_date_on_file_suffix

from unaflow.dags.trigger_dag.trigger_dag_factory import trigger_dag_factory
from unaflow.dags.trigger_dag.providers.google.gcs_trigger_dag_configuration import GcsMovefilesTriggerDagConfiguration

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


class GcsTriggerOnFilename(GcsMovefilesTriggerDagConfiguration):
    def execution_date(*args, **kwargs):
        return evaluate_execution_date_on_file_suffix(kwargs['ti'])

    def create_downstream_triggering(self):
        return BashOperator(
            task_id="the_end",
            bash_command="echo the end"
        )

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
        single_file=True,
    ),
    start_date=hello_world.start_date,
)
