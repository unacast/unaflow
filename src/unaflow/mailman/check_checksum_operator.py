from airflow import AirflowException
from airflow.models import BaseOperator, DagRun
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class CheckChecksumOperator(BaseOperator):
    """
    Performs a check against the task run database for given checksum.
    If it finds a task with not the same run_id, but the same dag_id,
    task_id and checksum. Then it raises an AirflowException
    """

    template_fields = ['dag_id', 'task_id']
    ui_color = '#fff7e6'

    @apply_defaults
    def __init__(
            self,
            checksum,
            checksum_dag_id=None,
            checksum_task_id=None,
            *args, **kwargs):
        super(CheckChecksumOperator, self).__init__(*args, **kwargs)
        self.checksum = checksum
        self.checksum_dag_id = checksum_dag_id
        self.checksum_task_id = checksum_task_id

    @provide_session
    def execute(self, context, session=None):
        self.log.info("Checking '%s' for dag '%s' and task_id '%s'",
                      self.checksum, self.dag_id, self.task_id)

        TI = TaskInstance

        count = session.query(TI).filter(
            TI.dag_id == self.dag_id if self.checksum_dag_id is None else self.checksum_dag_id,
            TI.task_id == self.task_id if self.checksum_task_id is None else self.checksum_task_id,
            TI.state.in_(self.allowed_states),
            TI.execution_date.in_(dttm_filter),
            ).count()
        session.commit()
        return count == len(dttm_filter)
