from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago
# Operators; we need this to operate!
from operators.spark_kubernetes_operator import SparkKubernetesOperator
from operators.spark_kubernetes_sensor import SparkKubernetesSensor
import yaml

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1
}
# [END default_args]

# [START instantiate_dag]
spark_application_yaml = """
#
# Copyright 2017 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/spark-operator/spark:v2.4.4"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar"
  sparkVersion: "2.4.4"
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 2.4.4
    serviceAccount: default
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 2.4.4
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
"""
spark_application_dict = yaml.safe_load(spark_application_yaml)
spark_application_name = "spark-pi-{{ ds }}-{{ task_instance.try_number }}"
spark_application_dict['metadata']['name'] = spark_application_name

dag = DAG(
    'spark_pi',
    default_args=default_args,
    description='submit martgen on kubernetes',
    schedule_interval=timedelta(days=1),
)

t1 = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace="default",
    sparkapplication_object=spark_application_dict,
    dag=dag,
)

t2 = SparkKubernetesSensor(
    task_id='spark_pi_monitor',
    namespace="default",
    sparkapplication_name=spark_application_name,
    dag=dag
)
t1 >> t2
