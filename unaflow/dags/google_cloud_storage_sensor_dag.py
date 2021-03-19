from functools import partial

from airflow.models import TaskInstance
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.operators.python_operator import PythonOperator
from unaflow.dags.sensor_dag import AbstractSensorDAG

from unaflow.operators.gcs_copy import GoogleCloudStorageCopyOperator
from unaflow.sensors.gcs_sensor import GoogleCloudStoragePrefixSensorUnaflow


class GoogleCloudStoragePrefixSensorDAG(AbstractSensorDAG):
    """
    Creates a "Sensor-DAG" that checks for the existence of objects with a prefix.
    This DAG can also be configured to move files before triggering another DAG.
    When triggering another DAG the file(s) that triggered the event are passed to
    the trigger_dag_id, as configuration "filename" or "filenames" if single_file
    is set to False

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: str
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :type prefix: str
    :param single_file: If destination or destination_bucket is set, we move only one file
    :type single_file: bool
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param destination_bucket:
    :param destination:
    """

    def __init__(self,
                 bucket,
                 prefix,
                 single_file=True,
                 destination_bucket=None,
                 destination=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 max_results=20,
                 *args, **kwargs):

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.bucket = bucket
        self.prefix = prefix
        self.single_file = single_file
        self.destination_bucket = destination_bucket
        self.destination = destination
        self.max_results = max_results
        super(GoogleCloudStoragePrefixSensorDAG, self).__init__(*args, **kwargs)

        if destination or destination_bucket:
            self.top_file = PythonOperator(
                task_id='top_file',
                python_callable=partial(self.find_first, self.sensor.task_id),
                provide_context=True
            )
            if self.single_file:
                # Objects to move are the top file from the sensor
                source_objects = ["{{ ti.xcom_pull(task_ids='%s') }}" % self.top_file.task_id]
            else:
                # We move all the objects that triggered the sensor
                source_objects = "{{ ti.xcom_pull(task_ids='%s', key='files') }}" % self.sensor.task_id

            self.move_files = GoogleCloudStorageCopyOperator(
                task_id='move_files',
                source_bucket=self.bucket,
                source_objects=source_objects,
                destination_bucket=self.destination_bucket if self.destination_bucket else bucket,
                destination_object=destination,
                move_object=True,
                google_cloud_storage_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to
            )
            if self.single_file:
                # Choose only the top file
                self.sensor >> self.top_file >> self.move_files
            else:
                self.sensor >> self.move_files

            if self.trigger_dag_id and self.clear_dag:
                self.move_files >> self.check_if_retrigger_task
            elif self.trigger_dag_id:
                self.move_files >> self.trigger_dag

            self.move_files >> self.done

    def _create_sensor(self, task_id, mode, timeout, poke_interval):
        return GoogleCloudStoragePrefixSensorUnaflow(
            task_id=task_id,
            bucket=self.bucket,
            prefix=self.prefix,
            delegate_to=self.delegate_to,
            google_cloud_conn_id=self.gcp_conn_id,
            poke_interval=poke_interval,
            max_results=self.max_results,
            mode=mode
        )

    def payload(self, context, dro: DagRunOrder):
        dro = super().payload(context, dro)
        dro.payload.update({'bucket': self.destination_bucket if self.destination_bucket else self.bucket})

        if self.destination_bucket or self.destination:
            if self.single_file:
                # We only moved one file, so conf will contain filename instead of filenames
                dro.payload.update({'filename': context['ti'].xcom_pull(task_ids=self.move_files.task_id)[0]})
            else:
                dro.payload.update({'filenames': context['ti'].xcom_pull(task_ids=self.move_files.task_id)})
        else:
            dro.payload.update({'filenames': context['ti'].xcom_pull(task_ids=self.sensor.task_id)})

        return dro

    @staticmethod
    def find_first(sensor_id, **context):
        # We grab "this", meaning the task_instance
        task_instance: TaskInstance = context['ti']
        # Get the files that triggered the sensor
        file_list = task_instance.xcom_pull(task_ids=sensor_id, key='files')
        # Sort the files. Just to make some sense of out things
        sorted_file_list = sorted(file_list)
        # Return the top file to the xcom_of this task_instance
        return sorted_file_list[0]
