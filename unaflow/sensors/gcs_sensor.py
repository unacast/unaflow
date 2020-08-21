from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStoragePrefixSensorUnaflow(BaseSensorOperator):
    """
    Checks for the existence of a objects at prefix in Google Cloud Storage bucket.
    Added to xcom, on success, is a list of the files received for the prefix.
    PS! This is a workaround for Airflow versions who does not have the new
    GCSObjectsWtihPrefixExistenceSensor.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: str
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :type prefix: str
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('bucket', 'prefix')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix,
                 google_cloud_conn_id='google_cloud_default',
                 delegate_to=None,
                 max_results=20,
                 *args, **kwargs):
        super(GoogleCloudStoragePrefixSensorUnaflow, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.max_results = max_results
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info('Sensor checks existence of objects: %s, %s',
                      self.bucket, self.prefix)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)

        files = hook.list(self.bucket, prefix=self.prefix)
        found = bool(files)
        self.log.info("Found %s files. Returning %s to xcom" % (len(files), self.max_results))
        if found:
            context['ti'].xcom_push(key='files', value=files[:self.max_results])
        return found
