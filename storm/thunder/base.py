"""
Base components of deployments and deployment functionality
"""
from abc import ABCMeta, abstractmethod
import collections
import datetime
import logging
import re

import libcloud.compute.base
import libcloud.compute.deployment
import libcloud.compute.types

from c4.utils.jsonutil import JSONSerializable
from c4.utils.util import (getFullModuleName, getModuleClasses)

from .client import AdvancedSSHClient


PARAMETER_DOC_REGEX = re.compile(r"\s*:(?P<docType>\w+)\s+(?P<name>\w+):\s+(?P<description>.+)", re.MULTILINE)


log = logging.getLogger(__name__)

class BaseNodeInfo(JSONSerializable):
    """
    Base node information object that contains name, public ip address and ssh password
    in a manner that is compatible to :class:`~libcloud.compute.base.Node` objects

    :param name: node name
    :type name: str
    :param publicIp: public ip of the node
    :type publicIp: str
    :param privateIp: private ip of the node
    :type privateIp: str
    :param password: SSH password
    :type password: str
    """
    def __init__(self, name, publicIp, privateIp=None, password=None):
        self.name = name
        self.public_ips = [publicIp]
        self.private_ips = [privateIp] if privateIp else []
        self.extra = {}
        if password:
            self.extra["password"] = password

class ClusterDeployment(libcloud.compute.deployment.Deployment, JSONSerializable):
    """
    Base cluster deployment class

    :param usePrivateIps: use private ip to connect to nodes instead of the public one
    :type usePrivateIps: bool
    """
    __metaclass__ = ABCMeta

    def __init__(self, usePrivateIps=False):
        self.usePrivateIps = usePrivateIps

    @abstractmethod
    def run(self, nodes):
        """
        Run cluster-wide deployment on speficied nodes

        :param nodes: the nodes
        :type nodes: [:class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`]
        :returns: deployment results
        :rtype: :class:`~DeploymentResults`
        """

# TODO: add JSONSerializable date to c4.utils
class Datetime(JSONSerializable):
    """
    JSON serializable datetime

    :param value: datetime
    :type value: :class:`datetime.datetime`
    """
    def __init__(self, value=None):
        if value:
            if not isinstance(value, datetime.datetime):
                raise ValueError("'{0}' does not match type '{1}'".format(value, datetime.datetime))
            self.value = value
        else:
            self.value = datetime.datetime.utcnow()

    def toJSONSerializable(self, includeClassInfo=False):
        """
        Convert object to some JSON serializable Python object such as
        str, list, dict, etc.

        :param includeClassInfo: include class info in JSON, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        :returns: JSON serializable Python object
        """
        formattedDateString = "{:%Y-%m-%dT%H:%M:%S}.{:03d}Z".format(self.value, self.value.microsecond // 1000)
        if includeClassInfo:
            serializableDict = {"value": formattedDateString}
            serializableDict[self.classAttribute] = self.typeAsString
            return serializableDict
        else:
            return formattedDateString

class Deployment(libcloud.compute.deployment.Deployment, JSONSerializable):
    """
    Base deployment class
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def run(self, node, client):
        """
        Runs this deployment task on node using the client provided.

        :param node: node
        :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        :param client: connected SSH client
        :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
        :returns: node
        :rtype: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        """

class DeploymentResult(JSONSerializable):
    """
    Result of a single deployment

    :param deployment: deployment
    :type deployment: :class:`~Deployment`
    :param node: node
    :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
    :param start: start time
    :type start: :class:`datetime.datetime`
    :param end: end time
    :type end: :class:`datetime.datetime`
    """
    def __init__(self, deployment, node, start, end):
        if not isinstance(deployment, Deployment):
            raise ValueError("'{0}' needs to be of type '{1}'".format(deployment, Deployment))
        self.deployment = deployment

        if not isinstance(node, (libcloud.compute.base.Node, BaseNodeInfo)):
            raise ValueError("'{0}' needs to be of type '{1}' or {2}".format(node, libcloud.compute.base.Node, BaseNodeInfo))

        # convert to base node info and make sure not to include the password
        self.node = BaseNodeInfo(
            node.name,
            node.public_ips[0] if node.public_ips else "",
            privateIp=node.private_ips[0] if node.private_ips else None
        )
        self.start = Datetime(start)
        self.end = Datetime(end)

class DeploymentErrorResult(DeploymentResult):
    """
    Result of a single deployment with an error

    :param deployment: deployment
    :type deployment: :class:`~Deployment`
    :param node: node
    :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
    :param start: start time
    :type start: :class:`datetime.datetime`
    :param end: end time
    :type end: :class:`datetime.datetime`
    :param error: error
    :type error: :class:`Exception`
    """
    def __init__(self, deployment, node, start, end, error):
        super(DeploymentErrorResult, self).__init__(deployment, node, start, end)
        if isinstance(error, DeploymentRunError):
            self.error = {
                "value": error.value
            }
            if error.status:
                self.error["status"] = error.status
            if error.stdout:
                self.error["stdout"] = error.stdout
            if error.stderr:
                self.error["stderr"] = error.stderr
        else:
            self.error = {
                "value": str(error)
            }

class DeploymentResults(JSONSerializable):
    """
    Deployment results

    :param results: deployment results
    :type results: [:class:`~DeploymentResult`]
    :param start: start time
    :type start: :class:`datetime.datetime`
    :param end: end time
    :type end: :class:`datetime.datetime`
    """
    def __init__(self, results, start, end):
        self.success = {}
        self.error = {}
        self.addResults(results)
        self.start = Datetime(start)
        self.end = Datetime(end)

    def addResult(self, result):
        """
        :param result: deployment result
        :type result: :class:`~DeploymentResult`
        """
        if result:
            if result.node.name not in self.success:
                self.success[result.node.name] = {}
            self.success[result.node.name][result.deployment.typeAsString] = result
        else:
            if result.node.name not in self.error:
                self.error[result.node.name] = {}
            self.error[result.node.name][result.deployment.typeAsString] = result

    def addResults(self, results):
        """
        :param results: deployment results
        :type results: [:class:`~DeploymentResult`]
        """
        for result in results:
            self.addResult(result)

    @property
    def numberOfErrors(self):
        """
        Number of deployments with errors
        """
        return sum([
            len(deploymentResults)
            for deploymentResults in self.error.values()
        ])

class DeploymentRunError(libcloud.compute.types.DeploymentError, JSONSerializable):
    """
    Exception raised in case a run command failed

    :param node:
    :type: node: :class:`~libcloud.compute.base.Node`
    :param exceptionString: exception string
    :type exceptionString: str
    :param status: status
    :type status: int
    :param stdout: standard ouput
    :type stdout: str
    :param stderr: standard error
    :type stderr: str
    """
    def __init__(self, node, exceptionString, status=None, stdout=None, stderr=None):
        # convert to base node info and make sure not to include the password
        baseNodeInfo = BaseNodeInfo(
            node.name,
            node.public_ips[0] if node.public_ips else "",
            privateIp=node.private_ips[0] if node.private_ips else None
        )
        error = [exceptionString]
        if status is not None:
            error.append("Status: {0}".format(status))
        if stdout is not None and stdout.strip():
            error.append("Output: {0}".format(stdout.strip()))
        if stderr is not None and stderr.strip():
            error.append("Error: {0}".format(stderr.strip()))
        value = "\n".join(error)
        super(DeploymentRunError, self).__init__(baseNodeInfo, original_exception=value)
        self.status = status
        self.stdout = stdout
        self.stderr = stderr

    def __repr__(self):
        return "<DeploymentRunError: node={0}, error={1}, driver={2}>".format(
            self.node.id if hasattr(self.node, "id") else self.node.name,
            self.value,
            self.driver)

class NodesInfoMap(JSONSerializable):
    """
    Nodes information mapping from name to :class:`~BaseNodeInfo` objects
    """
    def __init__(self):
        self.nodes = {}

    def __contains__(self, item):
        return item in self.nodes

    def __getitem__(self, key):
        return self.nodes[key]

    def __iter__(self):
        return iter(self.nodes)

    def __len__(self):
        return len(self.nodes)

    def items(self):
        """
        Node items
        """
        return self.nodes.items()

    def addNodes(self, nodes):
        """
        Add nodes

        :param nodes: the nodes
        :type nodes: [:class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`]
        """
        for node in nodes:
            self.add(node)

    def add(self, node):
        """
        Add node

        :param node: node
        :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        """
        if node.name in self.nodes:
            log.warn("'%s' not being added because it already exists", node.name)
        else:
            self.nodes[node.name] = BaseNodeInfo(
                node.name,
                node.public_ips[0] if node.public_ips else "",
                privateIp=node.private_ips[0] if node.private_ips else None,
                password=node.extra.get("password"))

    def getNodesByNames(self, names):
        """
        Get nodes by their names

        :param names: node names
        :type names: [str]
        :returns: nodes
        :rtype: [:class:`~BaseNodeInfo`]
        """
        # create short name to node mapping
        shortNameNodesInfoMap = {}
        for name, nodeInfo in sorted(self.items()):
            nameParts = name.split(".")
            shortName = nameParts[0]
            if shortName in shortNameNodesInfoMap:
                log.warn("short name '%s' already resolves to '%s' so '%s' will not be added to short name mapping",
                         shortName, shortNameNodesInfoMap[shortName].name, name)
            else:
                shortNameNodesInfoMap[nameParts[0]] = nodeInfo

        # get nodes by name
        nodes = []
        for nodeName in names:
            if nodeName in self:
                nodes.append(self[nodeName])
            elif nodeName in shortNameNodesInfoMap:
                nodes.append(shortNameNodesInfoMap[nodeName])
            else:
                log.error("'%s' is not a valid node name", nodeName)

        return nodes


def deploy(deploymentOrDeploymentList, nodeOrNodes, timeout=60, usePrivateIps=False):
    """
    Run specified deployment on the nodes

    :param deploymentOrDeploymentList: single deployment or deployment list
    :type deploymentOrDeploymentList: :class:`~libcloud.compute.deployment.Deployment` or [:class:`~libcloud.compute.deployment.Deployment`]
    :param nodeOrNodes: node or list of nodes
    :type nodeOrNodes: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo` or [:class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`]
    :param timeout: timeout in seconds
    :type timeout: int
    :param usePrivateIps: use private ip to connect to nodes instead of the public one
    :type usePrivateIps: bool
    :returns: deployment results
    :rtype: :class:`~DeploymentResults`
    """
    totalStart = datetime.datetime.utcnow()
    results = []

    # make sure we have a list of deployments
    if isinstance(deploymentOrDeploymentList, collections.Sequence):
        deployments = deploymentOrDeploymentList
    else:
        deployments = [deploymentOrDeploymentList]

    # make sure we have a list of nodes
    if isinstance(nodeOrNodes, collections.Sequence):
        nodes = nodeOrNodes
    else:
        nodes = [nodeOrNodes]

    if not deployments:
        log.error("Nothing to deploy")
        totalEnd = datetime.datetime.utcnow()
        return DeploymentResults(results, totalStart, totalEnd)

    deploymentNames = [deployment.typeAsString for deployment in deployments]

    for node in nodes:
        client = AdvancedSSHClient(node.private_ips[0] if usePrivateIps else node.public_ips[0],
                                   password=node.extra.get("password"),
                                   timeout=timeout)
        try:
            client.connect()
            for deployment in deployments:
                start = datetime.datetime.utcnow()
                try:
                    deployment.run(node, client)
                    end = datetime.datetime.utcnow()
                    result = DeploymentResult(deployment, node, start, end)
                    log.info("Running '%s.%s' on '%s' took %s",
                             deployment.__class__.__module__, deployment.__class__.__name__, node.name, end-start)

                except Exception as exception:
                    end = datetime.datetime.utcnow()
                    result = DeploymentErrorResult(deployment, node, start, end, exception)
                    log.error("Could not run '%s.%s' on '%s': %s",
                              deployment.__class__.__module__, deployment.__class__.__name__, node.name, exception)
                    log.exception(exception)

            client.close()
            results.append(result)
        except Exception as exception:
            # FIXME: add as deployment error to the results
            log.error("Could not run '%s' on '%s': %s", ",".join(deploymentNames), node.name, exception)
    totalEnd = datetime.datetime.utcnow()
    log.info("Running '%s' on %d nodes took %s", ",".join(deploymentNames), len(nodes), totalEnd-totalStart)
    return DeploymentResults(results, totalStart, totalEnd)

def getDeployments():
    """
    Get deployment classes

    :returns: deployment name to deployment class map
    :rtype: dict
    """
    deployments = {}
    import storm.deployments
    deploymentClasses = getModuleClasses(storm.deployments, Deployment) + getModuleClasses(storm.deployments, ClusterDeployment)
    for deployment in deploymentClasses:
        # get module name without the common storm.deployments prefix
        moduleName = getFullModuleName(deployment).replace("storm.deployments.", "")
        deployments["{0}.{1}".format(moduleName, deployment.__name__)] = deployment
    return deployments

def getDocumentation(item):
    """
    Get documentation information on the specified item

    :param item: a class, object, function, etc. with a doc string
    :returns: a dictionary with parsed information on description and parameters
    :rtype: dict
    """
    documentation = {
        "parameters": {}
    }
    descriptionLines = [
        line.strip()
        for line in item.__doc__.strip().splitlines()
        if line and not line.strip().startswith(":")
    ]
    documentation["description"] = "\n".join(descriptionLines)

    for match in PARAMETER_DOC_REGEX.finditer(item.__doc__):
        if match.group("name") not in documentation["parameters"]:
            documentation["parameters"][match.group("name")] = {}

        if match.group("docType") == "param":
            documentation["parameters"][match.group("name")]["description"] = match.group("description")
        if match.group("docType") == "type":
            documentation["parameters"][match.group("name")]["type"] = match.group("description")
    return documentation

