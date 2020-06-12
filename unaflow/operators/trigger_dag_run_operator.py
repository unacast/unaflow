# This is pretty much a copy of the TriggerDagRunOperator
# of Apache Airflow

import datetime
import json

import six
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.models import BaseOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class TriggerDagRunUnacastOperator(BaseOperator):
    """
    This mimics Airflows TriggerDagRunOperator with a new feature - returning the run id used.
    """
    template_fields = ('trigger_dag_id', 'execution_date')
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            python_callable=None,
            execution_date=None,
            *args, **kwargs):
        super(TriggerDagRunUnacastOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id

        if isinstance(execution_date, datetime.datetime):
            self.execution_date = execution_date.isoformat()
        elif isinstance(execution_date, six.string_types):
            self.execution_date = execution_date
        elif execution_date is None:
            self.execution_date = execution_date
        else:
            raise TypeError(
                'Expected str or datetime.datetime type '
                'for execution_date. Got {}'.format(
                    type(execution_date)))

    def execute(self, context):
        if self.execution_date is not None:
            run_id = 'trig__' + timezone.utcnow().isoformat()
            self.execution_date = timezone.parse(self.execution_date)
        dro = DagRunOrder(run_id=run_id)
        if self.python_callable is not None:
            dro = self.python_callable(context, dro)
        if dro:
            dag_run = trigger_dag(dag_id=self.trigger_dag_id,
                                  run_id=dro.run_id,
                                  conf=json.dumps(dro.payload),
                                  execution_date=self.execution_date,
                                  replace_microseconds=False)
            return dag_run.run_id
        else:
            self.log.info("Criteria not met, moving on")
