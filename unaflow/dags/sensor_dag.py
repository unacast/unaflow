from datetime import timedelta

from airflow import DAG
from airflow.contrib.sensors.bash_sensor import BashSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import timezone

from unaflow.operators.check_dag_run_operator import CheckDagRunOperator
from unaflow.operators.trigger_dag_run_operator import TriggerDagRunUnacastOperator, DagRunOrder
from unaflow.sensors.external_dag_run_sensor import ExternalDagRunSensor


def execution_date_now(*args, **kwargs):
    return timezone.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


def execution_time_now(*args, **kwargs):
    return timezone.utcnow()


class AbstractSensorDAG(DAG):
    '''
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
    '''

    def __init__(self,
                 dag_id=None,
                 mode='reschedule',
                 poke_interval=60 * 5,
                 timeout=60 * 60 * 24 * 7,  # One week
                 schedule_interval=timedelta(minutes=10),
                 trigger_dag_id: str = None,
                 trigger_dag_wait: bool = False,
                 trigger_dag_execution_callable=execution_time_now,
                 sensor_on_retry_callback=None,
                 sensor_retries=None,
                 trigger_dag_python_callable=None,
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
        self.sensor_on_retry_callback = sensor_on_retry_callback
        self.sensor_retries = sensor_retries
        self.trigger_dag_python_callable = trigger_dag_python_callable

        # Create the START sensor task
        self.sensor = self._create_sensor_with_defaults(mode=mode,
                                                        timeout=timeout,
                                                        poke_interval=poke_interval)

        # When all is DONE, we mark this as success
        # This DONE task ensures that the whole DAG is a success,
        # and because of wait_for_downstream, no other schedules are allowed to start
        self.done = DummyOperator(task_id="done", dag=self)
        self.sensor >> self.done

        # If trigger_dag_id is set, we create a task for triggering another DAG
        # If trigger_dag_wait we also wait for the triggered DAG to finish
        if trigger_dag_id:
            self.trigger_dag_execution_date = PythonOperator(
                task_id="evaluate_execution_date",
                python_callable=trigger_dag_execution_callable,
                provide_context=True,
                dag=self
            )

            self.trigger_dag = TriggerDagRunUnacastOperator(
                task_id="trigger_%s" % trigger_dag_id,
                trigger_dag_id=trigger_dag_id,
                python_callable=self.payload,
                execution_date='{{ ti.xcom_pull(task_ids="evaluate_execution_date") }}',
                dag=self
            )
            self.sensor >> self.trigger_dag_execution_date >> self.trigger_dag

            # Create a wait for operator if asked for
            if trigger_dag_wait:
                self.wait_for_triggered_dag = ExternalDagRunSensor(
                    task_id="wait_for_triggered_dag",
                    external_run_id='{{ ti.xcom_pull(task_ids="%s") }}' % self.trigger_dag.task_id,
                    dag=self
                )
                self.check_dag_run_for_success = CheckDagRunOperator(
                    task_id="check_for_success",
                    run_id='{{ ti.xcom_pull(task_ids="%s") }}' % self.trigger_dag.task_id,
                    dag=self
                )
                self.trigger_dag >> self.wait_for_triggered_dag >> self.check_dag_run_for_success >> self.done
            else:
                self.trigger_dag >> self.done

    def payload(self, context, dro: DagRunOrder):
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

    def _create_sensor_with_defaults(self, mode, timeout, poke_interval) -> BaseSensorOperator:
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


class BashSensorDAG(AbstractSensorDAG):
    def __init__(self,
                 bash_command,
                 *args,
                 **kwargs):
        self.bash_command = bash_command
        super(BashSensorDAG, self).__init__(*args, **kwargs)

    def _create_sensor(self, task_id, mode, timeout, poke_interval):
        return BashSensor(task_id=task_id,
                          timeout=timeout,
                          poke_interval=poke_interval,
                          bash_command=self.bash_command,
                          mode=mode)
