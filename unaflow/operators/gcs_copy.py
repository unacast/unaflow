import ast
from os.path import basename

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageCopyOperator(BaseOperator):
    """
    Copies objects from a bucket to another, with renaming if requested.

    :param source_bucket: The source Google cloud storage bucket where the
         object is. (templated)
    :type source_bucket: str
    :param source_objects: The source names of the objects to copy in the Google cloud
        storage bucket. (templated)
    :type source_object: str
    :param destination_bucket: The destination Google cloud storage bucket
        where the object should be. If the destination_bucket is None, it defaults
        to source_bucket. (templated)
    :type destination_bucket: str
    :param destination_object: If supplied, the basename of the source-object will
        will be appended to this object. (templated)
    :type destination_object: str
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :type move_object: bool
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param last_modified_time: When specified, the objects will be copied or moved,
        only if they were modified after last_modified_time.
        If tzinfo has not been set, UTC will be assumed.
    :type last_modified_time: datetime.datetime


    """
    template_fields = ('source_bucket', 'source_objects', 'destination_bucket',
                       'destination_object',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 source_bucket,
                 source_objects,
                 destination_bucket=None,
                 destination_object=None,
                 move_object=False,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 last_modified_time=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageCopyOperator,
              self).__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_objects = source_objects
        self.move_object = move_object
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.last_modified_time = last_modified_time

    def execute(self, context):

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        if self.destination_bucket is None:
            self.log.warning(
                'destination_bucket is None. Defaulting it to source_bucket (%s)',
                self.source_bucket)
            self.destination_bucket = self.source_bucket

        source_objects = ast.literal_eval(self.source_objects) if isinstance(self.source_objects,
                                                                             str) else self.source_objects
        files_copied = []
        for source_object in source_objects:
            if self.destination_object is None:
                destination_object = source_object
            else:
                separator = "" if self.destination_object[-1:] == "/" else "/"
                destination_object = "%s%s%s" % (self.destination_object,
                                                 separator,
                                                 basename(source_object))

            self._copy_single_object(hook=hook, source_object=source_object,
                                     destination_object=destination_object)
            files_copied.append(destination_object)

        return files_copied

    def _copy_single_object(self, hook, source_object, destination_object):
        if self.last_modified_time is not None:
            # Check to see if object was modified after last_modified_time
            if hook.is_updated_after(self.source_bucket,
                                     source_object,
                                     self.last_modified_time):
                self.log.debug("Object has been modified after %s ", self.last_modified_time)
                pass
            else:
                return

        self.log.info('Executing copy of gs://%s/%s to gs://%s/%s',
                      self.source_bucket, source_object,
                      self.destination_bucket, destination_object)

        hook.rewrite(self.source_bucket, source_object,
                     self.destination_bucket, destination_object)

        if self.move_object:
            hook.delete(self.source_bucket, source_object)
