from airflow import AirflowException
from airflow.models import BaseOperator, DagRun
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class CheckDagRunOperator(BaseOperator):
    """
    Performs a check against the dag run database for given states.
    Raises AirflowException if state is not in given states
    """

    template_fields = ['run_id']
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
            self,
            run_id,
            allowed_states=None,
            *args, **kwargs):
        super(CheckDagRunOperator, self).__init__(*args, **kwargs)
        self.run_id = run_id
        self.allowed_states = allowed_states or [State.SUCCESS]
        if not set(self.allowed_states) <= set(State.dag_states):
            raise ValueError(
                'Valid values for `allowed_states`: {}'.format(State.dag_states)
            )

    @provide_session
    def execute(self, context, session=None):
        self.log.info("Checking '%s' for status within %s", self.run_id, self.allowed_states)

        dag_run = DagRun.find(run_id=self.run_id)
        if not dag_run:
            raise AirflowException("Could not find any dag runs with id '{0}'"
                                   .format(self.run_id))
        elif not dag_run.state <= set(self.allowed_states):
            raise AirflowException("Could not find any dag runs with id '{0}' "
                                   "and state within '{1}' but '{2}'"
                                   .format(self.run_id, self.allowed_states, dag_run.state))
        else:
            self.log.info("DAG run with id '%s' checked out ok with state '%s'",
                          self.run_id, dag_run.state)
