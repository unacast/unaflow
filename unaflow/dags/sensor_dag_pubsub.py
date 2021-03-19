import json
from base64 import b64decode

from airflow.contrib.operators.pubsub_operator import PubSubSubscriptionCreateOperator
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from airflow.operators.dagrun_operator import DagRunOrder

from unaflow.dags.sensor_dag import AbstractSensorDAG


class PubSubSensorDAG(AbstractSensorDAG):
    """
    :param project: the GCP project ID for the subscription (templated)
    :type project: string
    :param subscription: the Pub/Sub subscription name. Do not include the
        full subscription path.
    :type subscription: string
    :param topic: the Pub/Sub topic name. If this is set, we will try to
        create the subscription if it does not exist
    :type topic: string
    :param ack_messages: If True, each message will be acknowledged
        immediately rather than by any downstream tasks
    :type ack_messages: bool
    :param ack_deadline_secs: Number of seconds that a subscriber has to
            acknowledge each message pulled from the subscription
    :type ack_deadline_secs: int
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request
        must have domain-wide delegation enabled.
    :type delegate_to: string
    """

    def __init__(self,
                 project,
                 subscription,
                 topic=None,
                 ack_messages=True,
                 gcp_conn_id='google_cloud_default',
                 ack_deadline_secs=10,
                 delegate_to=None,
                 *args, **kwargs):
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project = project
        self.subscription = subscription
        self.ack_messages = ack_messages
        super(PubSubSensorDAG, self).__init__(*args, **kwargs)
        if topic:
            self.create_subscription = PubSubSubscriptionCreateOperator(
                task_id='create_subscription',
                topic_project=self.project,
                topic=topic,
                subscription=self.subscription,
                subscription_project=self.project,
                ack_deadline_secs=ack_deadline_secs,
                gcp_conn_id=gcp_conn_id,
                fail_if_exists=False,
                delegate_to=self.delegate_to,
                wait_for_downstream=True
            )
            self.create_subscription >> self.sensor

    def _create_sensor(self, task_id, mode, timeout, poke_interval):
        return PubSubPullSensor(task_id=task_id,
                                subscription=self.subscription,
                                ack_messages=self.ack_messages,
                                max_messages=1,
                                gcp_conn_id=self.gcp_conn_id,
                                project=self.project,
                                poke_interval=poke_interval,
                                mode=mode)

    def payload(self, context, dro: DagRunOrder):
        pubsub_message = context['ti'].xcom_pull(task_ids=self.sensor.task_id)
        dro = super().payload(context, dro)
        # Pop the top, decode and add the data directly to the dict (we only get 1 message)
        data_bytes = b64decode(pubsub_message[0]['message']['data'])
        dro.payload.update(json.loads(data_bytes.decode('utf-8')))
        return dro
