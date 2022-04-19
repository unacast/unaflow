from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.task_group import TaskGroup
from unaflow.dags.trigger_dag.trigger_dag_configuration import \
    TriggerDagConfiguration


def trigger_dag_factory(config: TriggerDagConfiguration, **dag_kwargs):
    """
    A factory for creating a DAG that is able to listen for events, from
    a supplied sensor in the config.

    The factory ensures that we only trigger an external DAG once per event.

    It basically follows this DAG graph

                     -------------------------------------------
    wait_for_event-->| evaluate_execution_date --> trigger_dag |
                     -------------------------------------------

    You are able to override the evaluate_execution_date function, with the
     supplied config.
    You are also able to add extra tasks betweem wait_for_event and
     evaluate_execution_date, by overriding the sensor_downstreams function
    in the config. And you can add tasks after trigger_dag, by overriding the
    triggering_downstreams function in the config.

    param: config: The configuration to be used. The config needs to at least
    supply a sensor.
    type: config: TriggerDagConfiguration
    param: dag_kwargs: These are the extra arguments to set on the dag. Certain
     values are not able to be set, such as max_active_runs, catchup,
    dag_id abd schedule interval (set in config)
    """

    # Use default doc_md from config if nothing is set
    if 'doc_md' not in dag_kwargs:
        dag_kwargs['doc_md'] = config.default_doc_md()

    with DAG(
            dag_id=config.dag_id,
            catchup=False,
            max_active_runs=1,
            schedule_interval=config.schedule_interval,
            **dag_kwargs) as dag:
        # Create the START sensor task
        # Subclasses needs to implement the _create_sensor method
        # with the appropriate sensor
        sensor: BaseSensorOperator = config.create_sensor()
        sensor.wait_for_downstream = True
        sensor.depends_on_past = True  # Need to set this as well as wait_for_downstream
        if config.sensor_on_retry_callback:
            sensor.on_retry_callback = config.sensor_on_retry_callback
        if config.sensor_retries:
            sensor.retries = config.sensor_retries

        with TaskGroup(group_id="trigger_dag") as tg:
            # Get the timestamp to trigger. Either it is default, now,
            # or supplied from the callable trigger_dag_execution_callable
            trigger_dag_execution_date = PythonOperator(
                task_id="evaluate_execution_date",
                python_callable=config.execution_date,
                provide_context=True,
            )

            # Trigger the external DAG
            # Merge DAG configuration from Configuration
            # with some sensible defaults
            # This is the DAG conf that is by default sent to the
            trigger_dag = TriggerDagRunOperator(
                task_id=f"trigger_{config.trigger_dag_id}",
                trigger_dag_id=config.trigger_dag_id,
                conf=config.configuration,
                execution_date='{{ ti.xcom_pull(task_ids="%s") }}' % trigger_dag_execution_date.task_id,
                reset_dag_run=config.clear_dag,
            )

            trigger_dag_execution_date >> trigger_dag

        sensor >> tg

        # Set downstreams to the sensor, if given.
        # Also make sure they are added as upstreams
        # to triggering
        sensor_downstreams = config.create_downstream_sensor()
        if sensor_downstreams:
            sensor.set_downstream(sensor_downstreams)
            tg.set_upstream(sensor_downstreams)

        # Set downstreams to the triggering, if given.
        triggering_downstreams = config.create_downstream_triggering()
        if triggering_downstreams:
            tg.set_downstream(triggering_downstreams)

    return dag
