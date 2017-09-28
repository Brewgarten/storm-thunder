"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE

SSH related deployments
"""
import logging
import os
import re
import subprocess

from c4.utils.logutil import ClassLogger
from c4.utils.util import EtcHosts

from ..thunder import Deployment


log = logging.getLogger(__name__)

@ClassLogger
class AddAuthorizedKey(Deployment):
    """
    Add the specified public key to the ``authorized_keys`` of the guest

    :param publicKey: public key
    :type publicKey: str
    :param publicKeyPath: public key path
    :type publicKeyPath: str
    :param user: user
    :type user: str
    """
    def __init__(self, publicKey=None, publicKeyPath="~/.ssh/id_rsa.pub", user="root"):
        super(AddAuthorizedKey, self).__init__()
        if publicKey:
            self.publicKey = publicKey
        else:
            with open(os.path.expanduser(publicKeyPath)) as publicKeyFile:
                self.publicKey = publicKeyFile.read()
        self.user = user
        self.userHome = os.path.join("/home", user) if user != "root" else "/root"

    def run(self, node, client, usePrivateIps):
        """
        Runs this deployment task on node using the client provided.

        :param node: node
        :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        :param client: connected SSH client
        :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
        :param usePrivateIps: use private ip to connect to nodes instead of the public one
        :type usePrivateIps: bool
        :returns: node
        :rtype: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        """
        network = client.read("/etc/sysconfig/network")
        hostname = re.findall("HOSTNAME=(.+)", network)[0]

        sshDirectory = os.path.join(self.userHome, ".ssh")
        client.mkdir(sshDirectory)
        authorizedKeysPath = os.path.join(sshDirectory, "authorized_keys")
        if not client.isFile(authorizedKeysPath):
            client.touch(authorizedKeysPath)

        # check if public key already authorized
        authorizedKeys = client.read(authorizedKeysPath)
        alreadyAuthorized = False
        for authorizedKey in authorizedKeys.splitlines():
            if authorizedKey.strip() == self.publicKey.strip():
                self.log.debug("'%s' already has authorized key '%s'", hostname, self.publicKey)
                alreadyAuthorized = True
                break

        if not alreadyAuthorized:
            client.put(authorizedKeysPath, contents=self.publicKey, mode="a", chmod=0600)

        # make sure that ssh directory has correct owner
        if self.user != "root":
            client.run("chown -R {user}:{user} {sshDirectory}".format(user=self.user, sshDirectory=sshDirectory))

        return node

@ClassLogger
class AddKnownHost(Deployment):
    """
    Add the specified host public key to the ``known_hosts`` of the guest

    :param host: host name
    :type host: str
    :param hostPublicKey: host public key
    :type hostPublicKey: str
    :param user: user
    :type user: str
    """
    def __init__(self, host, hostPublicKey, user="root"):
        super(AddKnownHost, self).__init__()
        self.host = host
        self.hostPublicKey = hostPublicKey
        self.user = user
        self.userHome = os.path.join("/home", user) if user != "root" else "/root"

    def run(self, node, client, usePrivateIps):
        """
        Runs this deployment task on node using the client provided.

        :param node: node
        :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        :param client: connected SSH client
        :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
        :param usePrivateIps: use private ip to connect to nodes instead of the public one
        :type usePrivateIps: bool
        :returns: node
        :rtype: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        """
        network = client.read("/etc/sysconfig/network")
        hostname = re.findall("HOSTNAME=(.+)", network)[0]

        sshDirectory = os.path.join(self.userHome, ".ssh")
        client.mkdir(sshDirectory)
        knownHostsPath = os.path.join(sshDirectory, "known_hosts")
        if not client.isFile(knownHostsPath):
            client.touch(knownHostsPath)

        # check if host public key already known
        knownHosts = client.read(knownHostsPath)
        alreadyKnown = False
        for knownHost in knownHosts.splitlines():
            if knownHost.startswith(self.host):
                self.log.debug("'%s' already knows host '%s'", hostname, self.host)
                self.log.debug(knownHost)
                alreadyKnown = True
                break

        if not alreadyKnown:
            knownHostEntry = "{0} {1}".format(self.host, self.hostPublicKey)
            client.put(knownHostsPath, contents=knownHostEntry, mode="a")
            if "." in self.host:
                knownShortHostEntry = "{0} {1}".format(self.host[0:self.host.index(".")], self.hostPublicKey)
                client.put(knownHostsPath, contents=knownShortHostEntry, mode="a")

        # make sure that ssh directory has correct owner
        if self.user != "root":
            client.run("chown -R {user}:{user} {sshDirectory}".format(user=self.user, sshDirectory=sshDirectory))

        return node

@ClassLogger
class AddToEtcHosts(Deployment):
    """
    Add given host information to the /etc/hosts file

    :param ip: ip address
    :type ip: str
    :param hostnames: hostnames
    :type hostnames: [str]
    """
    def __init__(self, ip, hostnames):
        super(AddToEtcHosts, self).__init__()
        self.hostnames = hostnames
        self.ip = ip

    def run(self, node, client, usePrivateIps):
        """
        Runs this deployment task on node using the client provided.

        :param node: node
        :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        :param client: connected SSH client
        :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
        :param usePrivateIps: use private ip to connect to nodes instead of the public one
        :type usePrivateIps: bool
        :returns: node
        :rtype: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        """
        etcHosts = EtcHosts.fromString(client.read("/etc/hosts"))
        for hostname in self.hostnames:
            etcHosts.add(hostname, self.ip, replace=True)

        entry = etcHosts.toString()
        client.put("/etc/hosts", contents=entry)

        return node

@ClassLogger
class GenerateHostSSHKeys(Deployment):
    """
    Generate Host SSH keys on the guest

    """
    def __init__(self):
        super(GenerateHostSSHKeys, self).__init__()

    def run(self, node, client, usePrivateIps):
        """
        Runs this deployment task on node using the client provided.

        :param node: node
        :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        :param client: connected SSH client
        :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
        :param usePrivateIps: use private ip to connect to nodes instead of the public one
        :type usePrivateIps: bool
        :returns: node
        :rtype: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        """
        network = client.read("/etc/sysconfig/network")
        hostname = re.findall("HOSTNAME=(.+)", network)[0]

        client.mkdir("/etc/ssh/")
        if client.isFile("/etc/ssh/ssh_host_rsa_key.pub"):
            self.log.warn("'%s' already has existing key '/etc/ssh/ssh_host_rsa_key.pub'", hostname)

        else:
            # check for existing key pair
            privateKeyFileName = "/tmp/{0}".format(hostname)
            publicKeyFileName = "/tmp/{0}.pub".format(hostname)
            if os.path.exists(privateKeyFileName):
                os.remove(privateKeyFileName)
            if os.path.exists(publicKeyFileName):
                os.remove(publicKeyFileName)

            # generate key pair
            subprocess.call(["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-C", hostname, "-f", privateKeyFileName])
            client.upload(privateKeyFileName, "/etc/ssh/ssh_host_rsa_key")
            client.chmod("/etc/ssh/ssh_host_rsa_key", 0600)
            client.upload(publicKeyFileName, "/etc/ssh/ssh_host_rsa_key.pub")

            os.remove(privateKeyFileName)
            os.remove(publicKeyFileName)

        return node

@ClassLogger
class GenerateSSHKeys(Deployment):
    """
    Generate SSH keys on the guest

    :param user: user
    :type user: str
    """
    def __init__(self, user="root"):
        super(GenerateSSHKeys, self).__init__()
        self.user = user
        self.userHome = os.path.join("/home", user) if user != "root" else "/root"

    def run(self, node, client, usePrivateIps):
        """
        Runs this deployment task on node using the client provided.

        :param node: node
        :type node: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        :param client: connected SSH client
        :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
        :param usePrivateIps: use private ip to connect to nodes instead of the public one
        :type usePrivateIps: bool
        :returns: node
        :rtype: :class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`
        """
        network = client.read("/etc/sysconfig/network")
        hostname = re.findall("HOSTNAME=(.+)", network)[0]

        sshDirectory = os.path.join(self.userHome, ".ssh")
        client.mkdir(sshDirectory)
        privateKeyPath = os.path.join(sshDirectory, "id_rsa")
        publicKeyPath = os.path.join(sshDirectory, "id_rsa.pub")
        if client.isFile(publicKeyPath):
            self.log.warn("'%s' already has existing key '%s'", hostname, publicKeyPath)

        else:
            # check for existing key pair
            privateKeyFileName = "/tmp/{0}".format(hostname)
            publicKeyFileName = "/tmp/{0}.pub".format(hostname)
            if os.path.exists(privateKeyFileName):
                os.remove(privateKeyFileName)
            if os.path.exists(publicKeyFileName):
                os.remove(publicKeyFileName)

            # generate key pair
            subprocess.call(["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-C", hostname, "-f", privateKeyFileName])
            client.upload(privateKeyFileName, privateKeyPath)
            client.chmod(privateKeyPath, 0600)
            client.upload(publicKeyFileName, publicKeyPath)

            os.remove(privateKeyFileName)
            os.remove(publicKeyFileName)

        # make sure that ssh directory has correct owner
        if self.user != "root":
            client.run("chown -R {user}:{user} {sshDirectory}".format(user=self.user, sshDirectory=sshDirectory))

        return node
