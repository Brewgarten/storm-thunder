"""
Serializable configuration functionality

Deployment DSL
--------------

We support multiple types of the deployment syntax.

1. The fully Hjson parseable version where deployments are
encapsulated in ``{}``. For example:

.. code-block:: bash

    deployments: [
        {
            ssh.AddAuthorizedKey: {
                publicKeyPath: ~/.ssh/id_rsa.pub
                deploymentNodes: [
                    node1
                ]
            }
        }
        {
            software.UpdateKernel: {}
        }

.. note::

    In case of deployments without parameters(``software.UpdateKernel``) we still need to specify the
    emtpy parameter dictionary ``{}``

2. A simplified and more readable syntax without ``{}`` that also allows for deployments
without parameters

.. code-block:: bash

    deployments: [
        ssh.AddAuthorizedKey: {
            publicKeyPath: ~/.ssh/id_rsa.pub
            deploymentNodes: [
                node1
            ]
        }
        software.UpdateKernel
    ]

"""
import logging
import re

from c4.utils.hjsonutil import HjsonSerializable
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.utils.util import initWithVariableArguments


log = logging.getLogger(__name__)

@ClassLogger
class DeploymentInfos(HjsonSerializable, JSONSerializable):
    """
    Container for all the deployment information items

    :param deployments: list of deployments
    :type deployments: [:class:`~DeploymentInfo`]
    """
    def __init__(self, deployments):
        self.deployments = deployments

    @classmethod
    def fromHjson(cls, hjsonString, objectHook=None):
        """
        Load object from the specified Hjson string

        :param cls: the class to deserialize into
        :type cls: class
        :param hjsonString: a Hjson string
        :type hjsonString: str
        :param objectHook: a function converting a Hjson dictionary
            into a dictionary containing Python objects. If ``None``
            then the default :py:meth:`fromHjsonSerializable` is used
        :type objectHook: func
        :returns: object instance of the respective class
        """
        # check for regular syntax by looking if the { is present for deployment items
        regularSyntax = re.search(r"deployments\s*:\s*\[\s*\{", hjsonString, re.DOTALL)
        if regularSyntax:
            return super(DeploymentInfos, cls).fromHjson(hjsonString, objectHook=objectHook)

        # we need to perform custom parsing here since the syntax is not necessarily hjson compliant
        match = re.search(r"deployments\s*:(?P<deployments>.*)", hjsonString, re.DOTALL)
        # remove surrounding []
        rawDeploymentsString = match.group("deployments").strip().lstrip("[").rstrip("]")
        # remove comments
        rawDeploymentsString = re.sub("#.*$", "", rawDeploymentsString, flags=re.MULTILINE)

        # use a simple token parser to find deployment items
        deploymentItems = []
        currentItem = []
        currentLevel = 0
        for match in re.finditer(r"(?P<token>\S+)", rawDeploymentsString, re.MULTILINE):
            token = match.group("token")
            currentItem.append(token)
            if "{" in token:
                currentLevel += 1
            elif "}" in token:
                currentLevel -= 1
                if currentLevel == 0:
                    # deal with deployments without parameters
                    while "{" not in currentItem[1]:
                        deploymentItems.append("{0}: {1}".format(currentItem.pop(0), "{}"))
                    deploymentItems.append("\n".join(currentItem))
                    currentItem[:] = []

        # deal with remaining deployments without parameters
        if currentItem:
            deploymentItems.extend("{0}: {1}".format(item, "{}") for item in currentItem)

        # reassemble the deployments string
        deploymentString = "deployments: [{0}]".format("\n".join("{ " + deploymentPart + " }" for deploymentPart in deploymentItems))

        # replace original deployments string with the parsed one
        deploymentString = re.sub(r"deployments\s*:\s*\[.*\]", deploymentString, hjsonString, flags=re.DOTALL)

        return super(DeploymentInfos, cls).fromHjson(deploymentString, objectHook=objectHook)

    @classmethod
    def fromHjsonSerializable(cls, hjsonDict):
        """
        Convert a dictionary from Hjson into a respective Python
        objects. By default the dictionary is returned as is.

        :param cls: the class to deserialize into
        :type cls: class
        :param hjsonDict: the Hjson dictionary
        :type hjsonDict: dict
        :returns: modified dictionary or Python objects
        """
        if "deployments" in hjsonDict:
            deploymentItems = hjsonDict.pop("deployments")
            deployments = []
            for deploymentItem in deploymentItems:
                deploymentName = deploymentItem.keys()[0]
                try:
                    deploymentInfo = DeploymentInfo.fromHjsonSerializable(deploymentItem)
                    if deploymentInfo:
                        deployments.append(deploymentInfo)
                except Exception as exception:
                    log.error("Could not load deployment '%s' because '%s'", deploymentName, exception)
                    log.exception(exception)
            return cls(deployments)
        return hjsonDict

class DeploymentInfo(HjsonSerializable, JSONSerializable):
    """
    Deployment information

    :param deployment: deployment
    :type deployment: :class:`~Deployment`
    :param nodes: the nodes on which to run the deployment
    :type nodes: [str]
    """
    def __init__(self, deployment, *nodes):
        self.deployment = deployment
        self.nodes = nodes if nodes else None

    @classmethod
    def fromHjsonSerializable(cls, hjsonDict):
        """
        Convert a dictionary from Hjson into a respective Python
        objects. By default the dictionary is returned as is.

        :param cls: the class to deserialize into
        :type cls: class
        :param hjsonDict: the Hjson dictionary
        :type hjsonDict: dict
        :returns: modified dictionary or Python objects
        """
        # deal with deployments without parameters
        if isinstance(hjsonDict, str):
            deploymentClassName = hjsonDict
            parameters = {}
        else:
            deploymentClassName, parameters = hjsonDict.popitem()

        nodes = parameters.get("deploymentNodes", None)
        if nodes:
            nodes = getTypedParameter(parameters, "deploymentNodes", [str])

        fullDeploymentClassName = "storm.deployments.{0}".format(deploymentClassName)

        # get class info
        info = fullDeploymentClassName.split(".")
        className = info.pop()
        moduleName = ".".join(info)

        # load class from module
        module = __import__(moduleName, fromlist=[className])
        deploymentClass = getattr(module, className)
        deployment = initWithVariableArguments(deploymentClass, **parameters)

        if nodes:
            return cls(deployment, *nodes)
        else:
            return cls(deployment)

def getTypedParameter(dictionary, name, expectedType, default=None):
    """
    Get parameter from the specified dictionary while making sure it matches
    the expected type

    :param dictionary: dictionary
    :type dictionary: dict
    :param name: parameter name
    :type name: str
    :param expectedType: expected type
    :type expectedType: type
    :param default: default parameter value
    """
    value = dictionary.pop(name, default)
    if isinstance(expectedType, list):
        if not isinstance(value, list):
            raise ValueError("Invalid value '{value}' for '{name}' parameter (needs to be a list)".format(
                value=value, name=name))
        expectedItemType = expectedType[0]
        for item in value:
            if not isinstance(item, expectedItemType):
                raise ValueError("Invalid value '{value}' for item in '{name}' parameter (needs to be a {expectedType})".format(
                    value=item, name=name, expectedType=expectedItemType.__name__))
    else:
        if not isinstance(value, expectedType):
            raise ValueError("Invalid value '{value}' for '{name}' parameter (needs to be a {expectedType})".format(
                value=value, name=name, expectedType=expectedType.__name__))
    return value

