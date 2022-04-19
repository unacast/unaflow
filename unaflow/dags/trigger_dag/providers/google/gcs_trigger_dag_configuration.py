from unaflow.dags.trigger_dag.trigger_dag_configuration import SENSOR_TASK_ID, TriggerDagConfiguration
from unaflow.operators.gcs_copy import GoogleCloudStorageCopyOperator

from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.sensors.base import BaseSensorOperator
from typing import Sequence, Union
from airflow.models.taskmixin import TaskMixin


XCOM_TOP_FILE = f"{{{{ ti.xcom_pull(task_ids='{SENSOR_TASK_ID}')[0] }}}}"


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
                 single_file=True,
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
        self.single_file = single_file
        if self.single_file:
            self._source_objects = [XCOM_TOP_FILE]
        else:
            self._source_objects = f"{{{{ ti.xcom_pull(task_ids='{SENSOR_TASK_ID}') }}}}"

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

    def create_downstream_sensor(self) -> Union[TaskMixin, Sequence[TaskMixin]]:
        return GoogleCloudStorageCopyOperator(
            task_id="move_files",
            source_bucket=self.bucket,
            source_objects=self._source_objects,
            destination_bucket=self.destination_bucket,
            destination_object=self.destination,
            move_object=True,
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

    def default_doc_md(self):
        return f"""# Triggering DAG for {self.trigger_dag_id}

This DAG listens for files arriving in `{self.bucket}` with the prefix `{self.prefix}`.

Once files arrives we move the file(s) into `{self.destination_bucket}/{self.destination}`
 before we trigger [{self.trigger_dag_id}](/tree?dag_id={self.trigger_dag_id})

The DAG to trigger is supplied the file and information about the triggering dag.

## On failure

Should the DAG fail, the next schedule will start, but it will halt at the first task `{SENSOR_TASK_ID}`,
 since the previous run was a failure.

To fix a failure, you will need to clear the failure, if it is clearable, and then mark the succeeding
DAG run as a failure. This will re-start the DAG run which failed.

If the task that failed is not "clearable", you need to either mark the failed task as succes,
or delete the DAG run, in order to un-clog the DAG."""
