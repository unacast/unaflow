from datetime import timedelta
from typing import Callable, Dict, Optional

from airflow import DAG
from airflow.operators.dagrun_operator import DagRunOrder, TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import timezone

from unaflow.operators.branch_dag_run_operator import BranchDagRunOperator
from unaflow.operators.clear_dag_operator import ClearDagOperator


def execution_date_now(*args, **kwargs):
    return timezone.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


def execution_time_now(*args, **kwargs):
    return timezone.utcnow()


class AbstractSensorDAG(DAG):
    """
    This is a DAG for triggering other dags, based on a sensor.

    :param poke_interval: Time in seconds that the job should wait in
        between each tries. Default is 5 minutes.
    :type poke_interval: int
    :param timeout: Time, in seconds before the sensor task times out and fails.
        Think of it as the time within we expect an event to happen.
        Default is one week.
    :type timeout: int
    :param mode: How the sensor operates. (Only for versions above 1.10.2)
        Options are: ``{ poke | reschedule }``, default is ``reschedule``.
        When set to ``poke`` the sensor is taking up a worker slot for its
        whole execution time and sleeps between pokes. Use this mode if the
        expected runtime of the sensor is short or if a short poke interval
        is required. Note that the sensor will hold onto a worker slot and
        a pool slot for the duration of the sensor's runtime in this mode.
        When set to ``reschedule`` the sensor task frees the worker slot when
        the criteria is not yet met and it's rescheduled at a later time. Use
        this mode if the time before the criteria is met is expected to be
        quite long. The poke interval should be more than one minute to
        prevent too much load on the scheduler.
    :type mode: str
    """

    def __init__(
            self,
            dag_id=None,
            mode: str = 'reschedule',
            poke_interval: Optional[int] = 60 * 5,
            timeout: Optional[int] = 60 * 60 * 24 * 7,  # One week
            schedule_interval: timedelta = timedelta(minutes=10),
            trigger_dag_id: Optional[str] = None,
            trigger_dag_execution_callable: Optional[Callable[[Dict, DagRunOrder], DagRunOrder]] = execution_time_now,  # noqa: E501
            sensor_on_retry_callback: Optional[Callable[[Dict], None]] = None,
            sensor_retries: Optional[int] = None,
            trigger_dag_python_callable=None,
            clear_dag=False,
            *args,
            **kwargs):
        if not dag_id and trigger_dag_id:
            dag_id = "%s_trigger" % trigger_dag_id
        super(AbstractSensorDAG, self).__init__(
            dag_id=dag_id,
            catchup=False,
            max_active_runs=1,
            schedule_interval=schedule_interval,
            *args, **kwargs)
        self.mode = mode
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.trigger_dag_id = trigger_dag_id
        self.trigger_dag_python_callable = trigger_dag_python_callable
        self.trigger_dag_execution_callable = trigger_dag_execution_callable
        self.sensor_on_retry_callback = sensor_on_retry_callback
        self.sensor_retries: int = sensor_retries
        self.clear_dag = clear_dag

        # Create the START sensor task
        # Subclasses needs to implement the _create_sensor method
        # with the appropriate sensor
        self.sensor = self.__create_sensor_with_defaults(mode=mode,
                                                         timeout=timeout,
                                                         poke_interval=poke_interval)

        # When all is DONE, we mark this as success
        # This DONE task ensures that the whole DAG is a success,
        # and because of wait_for_downstream, no other schedules are allowed to start
        self.done = DummyOperator(task_id="done", dag=self)
        self.sensor >> self.done

        # If trigger_dag_id is set, we create a task for triggering another DAG
        if trigger_dag_id:
            self._create_trigger_dag_tasks()

    def payload(self, context, dro: DagRunOrder):
        """
        An overridable function that adds information to the trigger.
        Add extra payload in the dro.payload object if needed.
        """
        if self.trigger_dag_python_callable:
            dro = self.trigger_dag_python_callable(context, dro)
        else:
            dro.payload = {}

        dro.payload.update({
            'trigger_run_id': context['run_id'],
            'trigger_dag': context['dag'].dag_id,
            'trigger_dag_task': context['task'].task_id,
            'trigger_dag_task_url': context['ti'].log_url
        })

        return dro

    def __create_sensor_with_defaults(self, mode, timeout, poke_interval) -> BaseSensorOperator:
        sensor = self._create_sensor(task_id="wait_for_event",
                                     mode=mode,
                                     timeout=timeout,
                                     poke_interval=poke_interval)
        sensor.dag = self
        sensor.wait_for_downstream = True
        sensor.depends_on_past = True  # Need to set this as well as wait_for_downstream
        if self.sensor_on_retry_callback:
            sensor.on_retry_callback = self.sensor_on_retry_callback
        if self.sensor_retries:
            sensor.retries = self.sensor_retries

        return sensor

    def _create_sensor(self, task_id, mode, timeout, poke_interval) -> BaseSensorOperator:
        raise NotImplementedError("Need to implement sensor class in subclass")

    def _create_trigger_dag_tasks(self):
        # Get the timestamp to trigger. Either it is default, now,
        # or supplied from the callable trigger_dag_execution_callable
        self.trigger_dag_execution_date = PythonOperator(
            task_id="evaluate_execution_date",
            python_callable=self.trigger_dag_execution_callable,
            provide_context=True,
            dag=self
        )

        self.trigger_dag = TriggerDagRunOperator(
            task_id="trigger_%s" % self.trigger_dag_id,
            trigger_dag_id=self.trigger_dag_id,
            python_callable=self.payload,
            execution_date='{{ ti.xcom_pull(task_ids="evaluate_execution_date") }}',
            dag=self
        )
        if self.clear_dag:
            self.clear_dag_task = ClearDagOperator(
                task_id="clear_dag",
                execution_dag_id=self.trigger_dag_id,
                execution_date='{{ ti.xcom_pull(task_ids="evaluate_execution_date") }}',
                dag=self
            )

            self.check_if_retrigger_task = BranchDagRunOperator(
                task_id="check_if_retrigger",
                execution_dag_id=self.trigger_dag_id,
                execution_date='{{ ti.xcom_pull(task_ids="evaluate_execution_date") }}',
                branch_false=self.trigger_dag.task_id,
                branch_true=self.clear_dag_task.task_id,
                dag=self
            )

            self.sensor >> self.trigger_dag_execution_date >> self.check_if_retrigger_task
            self.check_if_retrigger_task >> [self.trigger_dag, self.clear_dag_task]
        else:
            self.sensor >> self.trigger_dag_execution_date >> self.trigger_dag
