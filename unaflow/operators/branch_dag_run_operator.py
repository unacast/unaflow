from typing import Union

from airflow.models import BaseOperator, SkipMixin, DagRun
from airflow.utils import timezone
from pendulum import datetime


class BranchDagRunOperator(BaseOperator, SkipMixin):
    template_fields = ['execution_dag_id', 'execution_date', ]

    """
    Allows a workflow to "branch" or follow a path following the execution
    of this task.
    """

    def __init__(self,
                 execution_dag_id: str,
                 execution_date: Union[str, datetime],
                 branch_true: str,
                 branch_false: str,
                 *args,
                 **kwargs
                 ):
        super(BranchDagRunOperator, self).__init__(*args, **kwargs)
        self.execution_dag_id = execution_dag_id
        self.execution_date = execution_date
        self.branch_true = branch_true
        self.branch_false = branch_false

    def execute(self, context):
        branch = self.check_dagrun_exists()
        self.skip_all_except(context['ti'], branch)

    def check_dagrun_exists(self):
        if isinstance(self.execution_date, str):
            date = timezone.parse(self.execution_date)
        else:
            date = self.execution_date

        runs = DagRun.find(
            dag_id=self.execution_dag_id,
            execution_date=date,
        )

        if len(runs) > 0:
            return self.branch_true
        else:
            return self.branch_false
