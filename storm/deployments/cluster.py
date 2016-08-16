"""
Cluster deployments
"""
import datetime
import logging

from c4.utils.logutil import ClassLogger

from ..thunder import (ClusterDeployment,
                       DeploymentRunError)
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
        for node in nodes:
            for deployment in deployments:
                deployment.run(node, clients[node.name])

        return nodes

@ClassLogger
class SetupPasswordlessSSH(ClusterDeployment):
    """
    Deploys passwordless SSH between nodes
    """
    def __init__(self):
        super(SetupPasswordlessSSH, self).__init__()

    def run(self, nodes, clients):
        hostKeys = {}
        sshKeys = []

        self.log.info("Getting ssh keys from hosts in the cluster")
        for node in nodes:
            try:
                GenerateHostSSHKeys().run(node, clients[node.name])
                GenerateSSHKeys().run(node, clients[node.name])
                hostKeys[node.name] = clients[node.name].read("/etc/ssh/ssh_host_rsa_key.pub")
                sshKeys.append(clients[node.name].read('/root/.ssh/id_rsa.pub'))
            except Exception as exception:
                raise DeploymentRunError(node, "Could not get ssh keys from '{0}': {1}".format(node.name, exception))

        self.log.info("Deploying passwordless SSH")
        for node in nodes:
            try:
                start = datetime.datetime.utcnow()
                for host, key in hostKeys.items():
                    AddKnownHost(host, key).run(node, clients[node.name])
                for sshKey in sshKeys:
                    AddAuthorizedKey(sshKey).run(node, clients[node.name])
                end = datetime.datetime.utcnow()
                log.info("Running 'passwordless SSH deployment' on '%s' nodes took %s", node.name, end-start)
            except Exception as exception:
                raise DeploymentRunError(node, "Could not run 'passwordless SSH deployment' on '{0}': {1}".format(node.name, exception))

        return nodes
