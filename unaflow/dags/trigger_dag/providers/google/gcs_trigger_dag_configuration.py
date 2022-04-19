from unaflow.dags.trigger_dag.providers.google.gcs_context_functions import find_first_file
from unaflow.dags.trigger_dag.trigger_dag_configuration import SENSOR_TASK_ID, TriggerDagConfiguration
from unaflow.operators.gcs_copy import GoogleCloudStorageCopyOperator

from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.sensors.base import BaseSensorOperator
from typing import Sequence, Union
from airflow.models.taskmixin import TaskMixin
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator


class GcsMovefilesTriggerDagConfiguration(TriggerDagConfiguration):
    """
    Parameters for Google Cloud Storage, that checks for the existence of objects
     with a prefix.
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
                 bucket: str = None,
                 prefix: str = None,
                 destination_bucket=None,
                 destination=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs
                 ) -> None:
        super(GcsMovefilesTriggerDagConfiguration, self).__init__(*args, **kwargs)
        self.bucket: str = bucket
        self.prefix: str = prefix
        self.destination_bucket = destination_bucket or bucket
        self.destination = destination
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.configuration = {**self.configuration, 'bucket': self.destination_bucket}

    def create_sensor(self) -> BaseSensorOperator:
        return GCSObjectsWithPrefixExistenceSensor(
            task_id=SENSOR_TASK_ID,
            bucket=self.bucket,
            prefix=self.prefix,
            delegate_to=self.delegate_to,
            google_cloud_conn_id=self.gcp_conn_id,
            poke_interval=self.poke_interval,
            mode=self.mode
        )

    def _on_failure_doc_md(self):
        return f"""## On failure

Should the DAG fail, the next schedule will start, but it will halt at the first task `{SENSOR_TASK_ID}`,
 since the previous run was a failure.

To fix a failure, you will need to clear the failure, if it is clearable, and then mark the succeeding
DAG run as a failure. This will re-start the DAG run which failed.

If the task that failed is not "clearable", you need to either mark the failed task as succes,
or delete the DAG run, in order to un-clog the DAG."""


class GcsMovefilesTriggerDagConfigurationSingle(GcsMovefilesTriggerDagConfiguration):
    TASK_ID_TOP_FILE = "top_file"
    TASK_GROUP_ID = "move_file"
    TASK_ID_TOP_FILE_FULL = f'{TASK_GROUP_ID}.{TASK_ID_TOP_FILE}'
    TASK_ID_MOVE_FILE_FULL = f'{TASK_GROUP_ID}.{TASK_GROUP_ID}'

    def __init__(self, *args, **kwargs) -> None:
        super(GcsMovefilesTriggerDagConfigurationSingle, self).__init__(*args, **kwargs)
        self.configuration = {**self.configuration,
                              'filename': f"{{{{ ti.xcom_pull(task_ids='{self.TASK_ID_MOVE_FILE_FULL}') }}}}"}

    def create_downstream_sensor(self) -> Union[TaskMixin, Sequence[TaskMixin]]:
        with TaskGroup(group_id="move_file") as tg:
            top_file = self.create_find_top_file_task()

            move_files = GoogleCloudStorageCopyOperator(
                task_id=self.TASK_GROUP_ID,
                source_bucket=self.bucket,
                source_objects=[f"{{{{ ti.xcom_pull(task_ids='{self.TASK_ID_TOP_FILE_FULL}') }}}}"],
                destination_bucket=self.destination_bucket,
                destination_object=self.destination,
                move_object=True,
                google_cloud_storage_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to
            )

            top_file >> move_files
        return tg

    def create_find_top_file_task(self):
        return PythonOperator(
            task_id=self.TASK_ID_TOP_FILE,
            python_callable=find_first_file,
            provide_context=True
        )

    def default_doc_md(self):
        return f"""# Triggering DAG for {self.trigger_dag_id}

This DAG listens for files arriving in `{self.bucket}` with the prefix `{self.prefix}`.

Once files arrives, we sort the results and pick the first file. This file is moved
 into `{self.destination_bucket}/{self.destination}` before we trigger
 [{self.trigger_dag_id}](/tree?dag_id={self.trigger_dag_id})

The DAG to trigger is supplied the file and information about the triggering dag.
""" + super()._on_failure_doc_md()


class GcsMovefilesTriggerDagConfigurationMultiple(GcsMovefilesTriggerDagConfiguration):
    TASK_ID_MOVE_FILES = 'move_files'

    def __init__(self, *args, **kwargs) -> None:
        super(GcsMovefilesTriggerDagConfigurationMultiple, self).__init__(*args, **kwargs)
        self.configuration = {**self.configuration,
                              'filenames': f'{{{{ ti.xcom_pull(task_ids={self.TASK_ID_MOVE_FILES}) }}}}'}

    def create_downstream_sensor(self) -> Union[TaskMixin, Sequence[TaskMixin]]:
        return GoogleCloudStorageCopyOperator(
            task_id=self.TASK_ID_MOVE_FILES,
            source_bucket=self.bucket,
            source_objects="{{ ti.xcom_pull(task_ids='%s') }}" % SENSOR_TASK_ID,
            destination_bucket=self.destination_bucket,
            destination_object=self.destination,
            move_object=True,
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

    def default_doc_md(self):
        return f"""# Triggering DAG for {self.trigger_dag_id}

This DAG listens for files arriving in `{self.bucket}` with the prefix `{self.prefix}`.

Once files arrives, we move the files into `{self.destination_bucket}/{self.destination}`
before we trigger [{self.trigger_dag_id}](/tree?dag_id={self.trigger_dag_id})

The DAG to trigger is supplied the file and information about the triggering dag.
""" + super()._on_failure_doc_md()
