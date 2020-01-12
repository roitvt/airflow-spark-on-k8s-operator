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
from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from kubernetes.client.rest import ApiException

from operators.kubernetes_hook import Kuberneteshook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SparkKubernetesOperator(BaseOperator):
    """
    creates sparkApplication in kubernetes cluster

    """

    template_fields = ['sparkapplication_object', 'namespace', 'kube_config']
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 sparkapplication_object: str,
                 namespace: str = 'default',
                 kube_config: str = '',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sparkapplication_object = sparkapplication_object
        self.namespace = namespace
        self.kube_config = kube_config

    def execute(self, context):
        self.log.info("creating sparkApplication")
        kubernetes_hook = Kuberneteshook()
        kubernetes_hook.create_custom_resource_definition(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.namespace,
            plural="sparkapplications",
            crd_object=self.sparkapplication_object
        )
