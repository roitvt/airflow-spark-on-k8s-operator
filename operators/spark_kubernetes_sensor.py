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
from typing import Callable, Dict, Optional

from airflow.exceptions import AirflowException
from operators.kubernetes_hook import Kuberneteshook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SparkKubernetesSensor(BaseSensorOperator):
    """
    checks sparkapplication state on kubernetes


    :param sparkapplication_name: sparkapplication resource name
    :type http_conn_id: str
    :param namespace: the kubernetes namespace where the sparkapplication reside in
    :type method: str
    :param kube_config: kubeconfig file location
    :type endpoint: str
    """

    template_fields = ('sparkapplication_name', 'namespace', 'kube_config')
    INTERMEDIATE_STATES = ('SUBMITTED', 'RUNNING',)
    FAILURE_STATES = ('FAILED', 'SUBMISSION_FAILED', 'UNKNOWN')
    SUCCESS_STATES = 'COMPLETED'
    @apply_defaults
    def __init__(self,
                 sparkapplication_name: str,
                 namespace: str = 'default',
                 kube_config: str = '',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sparkapplication_name = sparkapplication_name
        self.namespace = namespace
        self.kube_config = kube_config
        self.hook = Kuberneteshook()

    def poke(self, context: Dict):
        self.log.info('Poking: %s', self.sparkapplication_name)
        crd_state = self.hook.get_custom_resource_definition(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.namespace,
            plural="sparkapplications",
            name=self.sparkapplication_name
        )
        sparkapplication_state = crd_state['status']['applicationState']['state']
        self.log.info('spark application state: {}'.format(sparkapplication_state))
        if sparkapplication_state in self.FAILURE_STATES:
            raise AirflowException('spark application failed with state: {}'.format(sparkapplication_state))
        if sparkapplication_state in self.INTERMEDIATE_STATES:
            self.log.info('spark application is still in state: {}'.format(sparkapplication_state))
            return False
        if sparkapplication_state in self.SUCCESS_STATES:
            self.log.info('spark application ended successfully')
            return True
        raise AirflowException('unknown spark application state: {}'.format(sparkapplication_state))
