# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance, DagBag, DagModel, DagRun
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
                'Valid values for `allowed_states`: {}'.format(State.dag_states)
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
