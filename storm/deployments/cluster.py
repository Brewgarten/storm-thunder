"""
Cluster deployments
"""
import logging
import os

from c4.utils.logutil import ClassLogger

from ..thunder import (ClusterDeployment,
                       DeploymentRunError,
                       deploy)
from .ssh import (AddAuthorizedKey, AddKnownHost, AddToEtcHosts,
                  GenerateHostSSHKeys, GenerateSSHKeys)


log = logging.getLogger(__name__)

@ClassLogger
class AddNodesToEtcHosts(ClusterDeployment):
    """
    Deploy node information by adding ip addresses and aliases to /etc/hosts

    :param privateIp: use private ip in /etc/hosts instead of the public one
    :type privateIp: bool
    """
    def __init__(self, privateIp=False):
        super(AddNodesToEtcHosts, self).__init__()
        self.privateIp = privateIp

    def run(self, nodes, clients):
        # gather node information from all nodes
        deployments = []
        for node in nodes:
            # get long and short hostnames
            nodeNameParts = node.name.split(".")
            hostnames = [node.name]
            if len(nodeNameParts) > 1:
                hostnames.append(nodeNameParts[0])

            # check which ip to use
            if self.privateIp:
                if node.private_ips:
                    deployments.append(AddToEtcHosts(node.private_ips[0], hostnames))
                else:
                    raise DeploymentRunError(node, "Node '{0}' does not have a private ip".format(node.name))
            else:
                deployments.append(AddToEtcHosts(node.public_ips[0], hostnames))

        # go through all nodes and add node information of all other nodes to /etc/hosts
        results = deploy(deployments, nodes)
        if results.numberOfErrors > 0:
            raise DeploymentRunError(nodes[0], results.toJSON(includeClassInfo=True, pretty=True))

        return nodes

@ClassLogger
class SetupPasswordlessSSH(ClusterDeployment):
    """
    Deploys passwordless SSH between nodes

    :param user: user
    :type user: str
    """
    def __init__(self, user="root"):
        super(SetupPasswordlessSSH, self).__init__()
        self.user = user
        self.userHome = os.path.join("/home", user) if user != "root" else "/root"

    def run(self, nodes, clients):
        hostKeys = {}
        sshKeys = []

        self.log.info("Getting ssh keys from hosts in the cluster")
        results = deploy([GenerateHostSSHKeys(), GenerateSSHKeys(user=self.user)], nodes)
        if results.numberOfErrors > 0:
            raise DeploymentRunError(nodes[0], results.toJSON(includeClassInfo=True, pretty=True))
        publicKeyPath = os.path.join(self.userHome, ".ssh", "id_rsa.pub")
        for node in nodes:
            try:
                hostKeys[node.name] = clients[node.name].read("/etc/ssh/ssh_host_rsa_key.pub")
                sshKeys.append(clients[node.name].read(publicKeyPath))
            except Exception as exception:
                raise DeploymentRunError(node, "Could not get ssh keys from '{0}': {1}".format(node.name, exception))

        self.log.info("Deploying passwordless SSH")
        deployments = []
        for host, key in hostKeys.items():
            deployments.append(AddKnownHost(host, key, user=self.user))
        for sshKey in sshKeys:
            deployments.append(AddAuthorizedKey(publicKey=sshKey, user=self.user))
        results = deploy(deployments, nodes)
        if results.numberOfErrors > 0:
            raise DeploymentRunError(nodes[0], results.toJSON(includeClassInfo=True, pretty=True))

        return nodes
