from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Dict, Optional, Sequence, Union

from airflow.models.taskmixin import TaskMixin
from airflow.models.baseoperator import TaskStateChangeCallback
from airflow.sensors.base import BaseSensorOperator
from airflow.utils import timezone

ONE_WEEK = 60 * 60 * 24 * 7
FIVE_MINUTES = 60 * 5

SENSOR_TASK_ID = "wait_for_event"


class TriggerDagConfiguration(ABC):
    """
    This is the parameters for triggering other dags, based on a supplied sensor.

    :param trigger_dag_id: The id of the DAG to trigger on succesful sensor (Templated)
    :type trigger_dag_id: str
    :param dag_id: The id of the DAG. If None it is trigger_dag_id+_trigger
    :type dag_id: str
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

    def __init__(self,
                 trigger_dag_id: str,
                 dag_id: str = None,
                 mode: str = 'reschedule',
                 poke_interval: Optional[int] = FIVE_MINUTES,
                 timeout: Optional[int] = ONE_WEEK,
                 schedule_interval: timedelta = timedelta(minutes=10),
                 sensor_on_retry_callback: Optional[TaskStateChangeCallback] = None,
                 sensor_retries: Optional[int] = None,
                 clear_dag=False,
                 configuration: Dict = None,
                 ) -> None:
        self.trigger_dag_id = trigger_dag_id
        self.dag_id = dag_id if dag_id else f'{trigger_dag_id}_trigger'
        self.mode = mode
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.schedule_interval = schedule_interval
        self.sensor_on_retry_callback = sensor_on_retry_callback
        self.sensor_retries = sensor_retries
        self.clear_dag = clear_dag
        self.configuration = configuration if configuration else {}
        # Update configuration with some sensible defaults
        self.configuration = {**self.configuration,
                              'trigger_run_id': '{{ run_id }}',
                              'trigger_dag': '{{ dag.dag_id }}',
                              'trigger_dag_task': '{{ task.task_id }}',
                              'trigger_dag_task_url': '{{ task_instance.log_url }}',
                              }

    def execution_date(*args, **kwargs):
        """
        Override this method to return the appropriate execution date
         for the DAG to trigger.

        Default is to evaluate to now.
        """
        return timezone.utcnow()

    @abstractmethod
    def create_sensor(self) -> BaseSensorOperator:
        """
        Override this and return the sensor that starts off the triggering
        """
        pass

    def create_downstream_sensor(self) -> Union[TaskMixin, Sequence[TaskMixin]]:
        """
        Override this and return tasks that should be downstreams of the sensor
        and upstreams of the triggering
        """
        pass

    def create_downstream_triggering(self) -> Union[TaskMixin, Sequence[TaskMixin]]:
        """
        Override this and return tasks that should be downstreams of triggering
        """
        pass

    def default_doc_md(self) -> str:
        """
        Override this and return a self aware doc_md.
        Good for default description of the pipeline.
        """
        pass
