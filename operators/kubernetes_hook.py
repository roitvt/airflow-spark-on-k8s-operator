from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from kubernetes.client.rest import ApiException

try:
    from kubernetes import client, config

except ImportError:
    raise ImportError('error importing kubernetes module')


class Kuberneteshook(BaseHook):
    """
    Interact with kubernetes API.

    :param kube_config: kubeconfig - kubernetes connection string
    :type kube_config: str
    """

    def __init__(
            self,
            kube_config=None,
    ):
        self.kube_config = kube_config

    def get_conn(self, api_kind=None):
        """
        Returns kubernetes api session for use with requests
        :param api_kind: kubernetes api type
        :type api_kind: string
        """
        if self.kube_config:
            config.load_kube_config(self.kube_config)
        else:
            config.load_kube_config()
        if api_kind == 'crd':
            api = client.CustomObjectsApi()
        return api

    def create_custom_resource_definition(self, group=None, version=None, namespace='default', plural=None, crd_object=None):
        r"""
        creates CRD object in kubernetes
        :param plural:
        :param version:
        :param group:
        :param crd_object: crd object definition
        :type crd_object: str
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        api = self.get_conn("crd")
        try:
            response = api.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                body=crd_object,
            )
            self.log.info(response)
            return response
        except ApiException as e:
            raise AirflowException("Exception when calling -> create_custom_resource_definition: %s\n" % e)

    def get_custom_resource_definition(self, group=None, version=None, namespace='default', plural=None, name=None):
        api = self.get_conn("crd")
        try:
            crd = api.get_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name
            )
            return crd
        except ApiException as e:
            raise AirflowException("Exception when calling -> get_custom_resource_definition: %s\n" % e)
