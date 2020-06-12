from airflow.models import DagRun
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class ExternalDagRunSensor(BaseSensorOperator):
    """
    Waits for a different DAG or a task in a different DAG to complete for a
    specific execution_date

    :param external_run_id: The run_id of the dag you wait for
    :type external_run_id: str
    :param allowed_states: list of allowed states, default is ``['success', 'failed']``
    :type allowed_states: list
    """
    template_fields = ['external_run_id']
    ui_color = '#19647e'

    @apply_defaults
    def __init__(self,
                 external_run_id,
                 allowed_states=None,
                 *args,
                 **kwargs):
        super(ExternalDagRunSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS, State.FAILED]
        if not set(self.allowed_states) <= set(State.dag_states):
            raise ValueError(
                'Valid values for `allowed_states`: %s ' % State.dag_states
            )
        self.external_run_id = external_run_id

    @provide_session
    def poke(self, context, session=None):
        self.log.info('Poking for %s... ', self.external_run_id)
        DR = DagRun

        count = session.query(DR).filter(
            DR.run_id == self.external_run_id,
            DR.state.in_(self.allowed_states),
        ).count()

        session.commit()
        return count == 1
