import datetime
import logging

from ..thunder import (AdvancedSSHClient,
                       ClusterDeployment,
                       deploy)
from .ssh import (AddAuthorizedKey, AddKnownHost, AddToEtcHosts,
                  GenerateHostSSHKeys, GenerateSSHKeys)


log = logging.getLogger(__name__)


class AddNodesToEtcHosts(ClusterDeployment):
    """
    Deploy node information by adding ip addresses and aliases to /etc/hosts

    :param privateIp: use private ip in /etc/hosts instead of the public one
    :type privateIp: bool
    :param usePrivateIps: use private ip to connect to nodes instead of the public one
    :type usePrivateIps: bool
    """
    def __init__(self, privateIp=False, usePrivateIps=False):
        super(AddNodesToEtcHosts, self).__init__(usePrivateIps=usePrivateIps)
        self.privateIp = privateIp

    def run(self, nodes):
        successNodes = []
        errorNodes = []

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
                    log.error("Node '%s' does not have a private ip", node.name)
                    errorNodes.append(node)
            else:
                deployments.append(AddToEtcHosts(node.public_ips[0], hostnames))

        if errorNodes:
            return successNodes, errorNodes

        # go through all nodes and add node information of all other nodes to /etc/hosts
        return deploy(deployments, nodes, usePrivateIps=self.usePrivateIps)

class SetupPasswordlessSSH(ClusterDeployment):
    """
    Deploys passwordless SSH to nodes

    :param usePrivateIps: use private ip to connect to nodes instead of the public one
    :type usePrivateIps: bool
    """
    def __init__(self, usePrivateIps=False):
        super(SetupPasswordlessSSH, self).__init__(usePrivateIps=usePrivateIps)

    def run(self, nodes):

        totalStart = datetime.datetime.utcnow()
        successNodes = []
        errorNodes = []
        hostKeys = {}
        sshKeys = []

        log.debug("Getting ssh keys from hosts in the cluster")
        for node in nodes:
            client = AdvancedSSHClient(node.private_ips[0] if self.usePrivateIps else node.public_ips[0], password=node.extra.get("password"), timeout=60)
            try:
                client.connect()
                GenerateHostSSHKeys().run(node, client)
                GenerateSSHKeys().run(node, client)
                hostKeys[node.name] = client.read("/etc/ssh/ssh_host_rsa_key.pub")
                sshKeys.append(client.read('/root/.ssh/id_rsa.pub'))
                client.close()
            except Exception as e:
                errorNodes.append(node)
                log.error("Could not get ssh keys from '%s': %s", node.name, e)

        log.debug("Deploying passwordless SSH")
        for node in nodes:
            client = AdvancedSSHClient(node.private_ips[0] if self.usePrivateIps else node.public_ips[0], password=node.extra.get("password"), timeout=60)
            try:
                start = datetime.datetime.utcnow()
                client.connect()
                for host, key in hostKeys.items():
                    AddKnownHost(host, key).run(node, client)
                for sshKey in sshKeys:
                    AddAuthorizedKey(sshKey).run(node, client)
                client.close()
                successNodes.append(node)
                end = datetime.datetime.utcnow()
                log.info("Running 'passwordless SSH deployment' on '%s' nodes took %s", node.name, end-start)
            except Exception as e:
                errorNodes.append(node)
                log.error("Could not run 'passwordless SSH deployment' on '%s': %s", node.name, e)
        totalEnd = datetime.datetime.utcnow()
        log.info("Running 'passwordless SSH deployment' on %d nodes took %s", len(nodes), totalEnd-totalStart)
        return successNodes, errorNodes
