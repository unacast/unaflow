from typing import Union

from airflow import DAG
from airflow.models import BaseOperator, DagBag
from airflow.utils import timezone, apply_defaults
from pendulum import datetime


class ClearDagOperator(BaseOperator):

    template_fields = ['execution_dag_id', 'execution_date']

    @apply_defaults
    def __init__(
            self,
            execution_dag_id: str,
            execution_date: Union[str, datetime],
            *args,
            **kwargs
    ):
        super(ClearDagOperator, self).__init__(*args, **kwargs)
        self.execution_dag_id = execution_dag_id
        self.execution_date = execution_date

    def execute(self, context):
        self.clear_dag()

    def clear_dag(self):
        if isinstance(self.execution_date, str):
            date = timezone.parse(self.execution_date)
        else:
            date = self.execution_date

        clear_dag: DAG = DagBag().get_dag(self.execution_dag_id)
        clear_dag.clear(
            start_date=date,
            end_date=date
        )
