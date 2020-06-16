import logging
import traceback

import dateutil
from airflow.contrib.hooks.opsgenie_alert_hook import OpsgenieAlertHook

log = logging.getLogger(__name__)


def report_failure_to_opsgenie(message: str = 'Oh no {task_id} in {dag_id} failed',
                               priority: str = 'P3',
                               tags=None,
                               alias=None,
                               responders=None,
                               team_id=None,
                               description='The exception was "{exception}"\nMore: {log_url}',
                               opsgenie_conn_id='opsgenie_default'
                               ):
    """
    Create a function capable of sending an alert to OpsGenie on failure. You can
    for example use this in the on_failure_callback of the DAG as so:
    ...
    on_failure_callback=report_failure_to_opsgenie(message="Houston we have a problem")
    ...
    The params message and description are templated. That means that you can string
    interpolate certain context variables. The variables you can use are:
    task_id: The id of the failed task
    dag_id: The id of the failed DAG
    timestamp: When the failure happened
    date_time: Date representation when the failure happened
    log_url: Airflow's url of the task log
    exception: The exception message
    stacktrace: Stacktrace of the exception
    job_id: The id of the task run
    mark_success_url: Url for directly marking the task as success

    default_args
    :param message: (Templated) Message of the alert
    :param priority: Priority level of the alert.
        Possible values are P1, P2, P3, P4 and P5. Default value is P3.
    :param tags: Tags of the alert.
    :param alias: Client-defined identifier of the alert,
        that is also the key element of Alert De-Duplication.
        See https://docs.opsgenie.com/docs/alert-deduplication
    :param responders: Teams, users, escalations and schedules that the alert
        will be routed to send notifications. type field is mandatory for each
        item, where possible values are team, user, escalation and schedule.
        If the API Key belongs to a team integration, this field will be overwritten
        with the owner team. Either id or name of each responder should be provided.
        You can refer below for example values.
    :param team_id: If this is set, it will create one responder with this id
    :param description: (Templated) Description field of the alert that is generally used to provide
        a detailed information about the alert.
    :param opsgenie_conn_id: The name of the Opsgenie connection to use
    :return: A function that can execute on failure context, and which itself returns
        OpsgenieAlertHook.execute result
    """

    def on_failure(context):
        logging.info("Hooking into OpsGenie")
        context_map = {'dag_id': context.get('task_instance').dag_id,
                       'task_id': context.get('task_instance').task_id,
                       'timestamp': context.get('ts'),  # Timestamp Example: 2018-01-01T00:00:00+00:00
                       'log_url': context.get('task_instance').log_url,
                       'date_time': dateutil.parser.parse(context.get('ts')),
                       'exception': "%s" % str(context['exception']).replace('"', '').replace("'", ""),
                       'stacktrace': '%s' % traceback.format_exc().replace('"', '').replace("'", ""),
                       'job_id': "%s" % context['task_instance'].job_id,
                       'mark_success_url': context.get('task_instance').mark_success_url
                       }
        _responders = [{"id": team_id, "type": "team"}] if team_id else responders
        json = {
            "message": message.format(**context_map),
            "description": description.format(**context_map),
            "responders": _responders,
            "tags": tags,
            "details": {
                "logUrl": context_map['log_url'],
                "stacktrace": context_map['stacktrace'],
                "jobId": context_map['job_id'],
                "dagId": context_map['dag_id'],
                "taskId": context_map['task_id']
            },
            "priority": priority,
            "alias": alias
        }

        logging.info("Trying to send OpsGenie this json: %s", json)

        hook = OpsgenieAlertHook(opsgenie_conn_id)
        return hook.execute(json)

    return on_failure
