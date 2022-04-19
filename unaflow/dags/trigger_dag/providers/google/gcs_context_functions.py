import re

from airflow.models import TaskInstance
from unaflow.dags.trigger_dag.trigger_dag_configuration import SENSOR_TASK_ID
from typing import Pattern
from airflow.utils import timezone


def find_first_file(ti: TaskInstance):
    """
    Finds the first result in the sorted xcom result of the sensor task.
    """
    # Get the files that triggered the sensor
    file_list = ti.xcom_pull(task_ids=SENSOR_TASK_ID)
    # Sort the files. Just to make some sense of out things
    sorted_file_list = sorted(file_list)
    # Return the top file to the xcom_of this task_instance
    return sorted_file_list[0]


def evaluate_execution_date_on_file_suffix(ti: TaskInstance, pattern: Pattern = re.compile('.*_([0-9]{8})')):
    """
    We utilise find_first_file and get the filename.
    From that filename, we derive the executiondate based on the suffix
    and the given pattern of the filename
    """

    filename = find_first_file(ti)
    match = pattern.match(filename)
    return str(timezone.parse(match.group(1), timezone=timezone.utc))
