"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE

Software related deployments
"""
import glob
import logging
import os
import re

from c4.utils.logutil import ClassLogger

from ..thunder import (ClusterDeployment,
                       Deployment,
                       DeploymentRunError,
                       RemoteTemporaryDirectory,
                       deploy)


log = logging.getLogger(__name__)

@ClassLogger
class ClusterDeployToDirectory(ClusterDeployment):
    """
    Deploy files to the specified directory and return the full
    remote paths of the files.

    This uses a fan-out approach so that the cost of uploading only occurs once

    :param directory: remote directory
    :type directory: str
    :param fileNames: file names
    :type fileNames: [str]
    """
    def __init__(self, directory, *fileNames):
        super(ClusterDeployToDirectory, self).__init__()
        self.directory = directory
        self.fileNames = []
        for fileName in fileNames:
            potentialFileNames = glob.glob(fileName)
            if not potentialFileNames:
                raise ValueError("'{0}' is an invalid path".format(fileName))
            for potentialFileName in potentialFileNames:
                if not os.path.exists(potentialFileName):
                    raise ValueError("'{0}' is an invalid path".format(potentialFileName))
                self.fileNames.append(potentialFileName)
        self.remoteFileNames = []

    def run(self, nodes, clients, usePrivateIps):
        """
        Run cluster-wide deployment on speficied nodes

        :param nodes: the nodes
        :type nodes: [:class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`]
        :param clients: node name to connected SSH client mapping
        :type clients: dict
        :param usePrivateIps: use private ip to connect to nodes instead of the public one
        :type usePrivateIps: bool
        :returns: nodes
        :rtype: [:class:`~libcloud.compute.base.Node` or :class:`~BaseNodeInfo`]
        """
        # client for the first node
        node = nodes[0]
        client = clients[node.name]

        UploadToDirectory(self.directory, *self.fileNames).run(node, client, usePrivateIps)

        deployments = []
        for fileName in self.fileNames:
            # TODO: make this more robust
            remoteFileName = os.path.join(self.directory, os.path.basename(fileName))
            deployments.append(RemoteCopy(node.name, self.directory, remoteFileName))

        results = deploy(deployments, nodes[1:], usePrivateIps=usePrivateIps)
        if results.numberOfErrors > 0:
            raise DeploymentRunError(nodes[0], results.toJSON(includeClassInfo=True, pretty=True))

        return nodes

@ClassLogger
class DeployPythonPackages(Deployment):
    """
    Upload and install Python package

    :param packages: the paths to packages
    :type packages: [str]
    :param pip: path to pip to use
    :type pip: str
    """
    def __init__(self, packages, pip="/usr/bin/pip"):
        super(DeployPythonPackages, self).__init__()
        self.packages = [
            packagePath.strip()
            for packagePath in packages
        ]
        self.pip = pip

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

        with RemoteTemporaryDirectory(client) as tmpDirectory:

            # upload packages
            uploadedPackages = [
                re.sub(r"-\d.*", r"", os.path.basename(packageName))
                for packageName in client.upload(self.packages, tmpDirectory)
            ]
            if len(uploadedPackages) != len(self.packages):
                raise DeploymentRunError(node, "Could not deploy Python packages")

            # install uploaded packages
            stdout, stderr, status = client.run("{pip} install --upgrade --force-reinstall --pre --no-index --find-links {tmpDirectory} {packages}".format(
                pip=self.pip,
                tmpDirectory=tmpDirectory,
                packages=" ".join(uploadedPackages)))
            if status != 0:
                raise DeploymentRunError(node, "Could not deploy Python packages", status, stdout, stderr)

        return node

@ClassLogger
class InstallLocalRPMPackages(Deployment):
    """
    Upload rpms and install them on the node.

    :param rpms: rpm file names
    :type rpms: [str]
    """
    def __init__(self, *rpms):
        super(InstallLocalRPMPackages, self).__init__()
        self.rpms = rpms

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
        # generate package to file mapping
        packages = {
            # remove rpm extension
            os.path.splitext(os.path.basename(fileName))[0] if fileName.endswith(".rpm") else os.path.basename(fileName): fileName
            for fileName in self.rpms
        }
        installedRPMInfo = isRPMPackageInstalled(client, *packages.keys())

        # collect the packages that are missing
        missingRPMs = []
        for packageName, installed in sorted(installedRPMInfo.items()):
            if installed:
                self.log.debug("Package '%s' already installed", packageName)
            else:
                missingRPMs.append(packages[packageName])

        # upload and install the missing packages
        with RemoteTemporaryDirectory(client) as tmpDirectory:
            uploadToDirectory = UploadToDirectory(tmpDirectory, *missingRPMs)
            uploadToDirectory.run(node, client)
            InstallRPMPackages(*uploadToDirectory.remoteFileNames).run(node, client, usePrivateIps=usePrivateIps)

        return node

@ClassLogger
class InstallRPMPackages(Deployment):
    """
    Download and install rpm using yum repositories

    :param rpms: rpm package names or rpm file names
    :type rpms: [str]
    """
    def __init__(self, *rpms):
        super(InstallRPMPackages, self).__init__()
        self.rpms = rpms

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
        if not self.rpms:
            return node
        for attempt in range(3):
            stdout, stderr, status = client.run("yum install --assumeyes {0}".format(" ".join(self.rpms)))
            if status != 0 and "[Errno 256] No more mirrors to try." in stderr:
                self.log.debug("Encountered yum cache mirror problem in attempt %d", attempt+1)
                client.run("yum clean all")
            else:
                break
        if status != 0:
            if "Nothing to do" in stderr:
                for match in re.finditer(r"/(?P<name>.*): does not update installed package.", stdout, re.MULTILINE):
                    self.log.debug("Package '%s' older than installed package", os.path.basename(match.group("name")))
            else:
                raise DeploymentRunError(node, "Could not yum install '{0}' packages.".format(",".join(self.rpms)), status, stdout, stderr)
        for match in re.finditer(r"Package (?P<name>.*) already installed", stdout, re.MULTILINE):
            self.log.debug("Package '%s' already installed", match.group("name"))

        return node

@ClassLogger
class RemoteCopy(Deployment):
    """
    Copy files from the specified node into the directory onto the current node

    :param ip: ip address
    :type ip: str
    :param nodeName: name of the node from which to copy
    :type nodeName: str
    :param directory: remote directory to copy into
    :type directory: str
    :param fileNames: file names to be copied
    :type fileNames: [str]
    """
    def __init__(self, nodeName, directory, *fileNames):
        super(RemoteCopy, self).__init__()
        self.nodeName = nodeName
        self.directory = directory
        self.fileNames = fileNames

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
        if not self.fileNames:
            return

        client.mkdir(self.directory)
        for fileName in self.fileNames:
            remoteFileName = os.path.join(self.directory, os.path.basename(fileName))
            if client.isFile(remoteFileName):
                self.log.debug("'%s' already uploaded", fileName)
            else:
                stdout, stderr, status = client.run("scp -pr root@{nodeName}:{source} {destination}".format(
                    nodeName=self.nodeName,
                    source=fileName,
                    destination=remoteFileName))
                if status != 0:
                    raise DeploymentRunError(node, "Could not remote copy '{0}' to {1}", status=status, stdout=stdout, stderr=stderr)

@ClassLogger
class RemoveRPMPackages(Deployment):
    """
    Remove rpm

    :param rpms: rpm package names or rpm file names
    :type rpms: [str]
    """
    def __init__(self, *rpms):
        super(RemoveRPMPackages, self).__init__()
        self.rpms = rpms

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
        if not self.rpms:
            return node
        for attempt in range(3):
            stdout, stderr, status = client.run("yum erase --assumeyes {0}".format(" ".join(self.rpms)))
            if status != 0 and "[Errno 256] No more mirrors to try." in stderr:
                self.log.debug("Encountered yum cache mirror problem in attempt %d", attempt+1)
                client.run("yum clean all")
            else:
                break
        if status != 0:
            raise DeploymentRunError(node, "Could not yum erase '{0}' packages.".format(",".join(self.rpms)), status, stdout, stderr)
        for match in re.finditer(r"No Match for argument: (?P<name>.*)", stderr, re.MULTILINE):
            self.log.debug("Package '%s' not installed", match.group("name"))

        return node

@ClassLogger
class UpdateLocalRPMPackages(Deployment):
    """
    Upload rpms to update existing ones on the node.

    :param rpms: rpm file names
    :type rpms: [str]
    """
    def __init__(self, *rpms):
        super(UpdateLocalRPMPackages, self).__init__()
        self.rpms = rpms

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
        with RemoteTemporaryDirectory(client) as tmpDirectory:
            uploadToDirectory = UploadToDirectory(tmpDirectory, *self.rpms)
            uploadToDirectory.run(node, client, usePrivateIps=usePrivateIps)
            UpdateRPMPackages(*uploadToDirectory.remoteFileNames).run(node, client, usePrivateIps=usePrivateIps)

        return node

@ClassLogger
class UpdateRPMPackages(Deployment):
    """
    Download and update rpms using yum repositories

    :param rpms: rpm package names or rpm file names
    :type rpms: [str]
    """
    def __init__(self, *rpms):
        super(UpdateRPMPackages, self).__init__()
        self.rpms = rpms

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
        if not self.rpms:
            return node
        for attempt in range(3):
            stdout, stderr, status = client.run("yum update --assumeyes {0}".format(" ".join(self.rpms)))
            if status != 0 and "[Errno 256] No more mirrors to try." in stderr:
                self.log.debug("Encountered yum cache mirror problem in attempt %d", attempt+1)
                client.run("yum clean all")
            else:
                break
        if status != 0:
            raise DeploymentRunError(node, "Could not yum update '{0}' packages.".format(",".join(self.rpms)), status, stdout, stderr)
        # note that update does not error out when packages are not updated
        if stderr.strip():
            self.log.error(stderr.strip())
        for match in re.finditer(r"/(?P<name>.*): does not update installed package.", stdout, re.MULTILINE):
            self.log.debug("Package '%s' already updated", os.path.basename(match.group("name")))

        return node

@ClassLogger
class UpdateKernel(Deployment):
    """
    Class to update kernel on nodes
    """
    def __init__(self):
        super(UpdateKernel, self).__init__()

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

        ## update kernel
        stdout, stderr, status = client.run("/usr/bin/yum update kernel --assumeyes")
        if status != 0:
            raise DeploymentRunError(
                node, "Could not update kernel", status, stdout, stderr)

        if "No Packages marked for Update" in stdout:
            return node

        ## reboot node to get the updated kernel
        stdout, stderr, status = client.run("/sbin/reboot")
        if status != 0:
            raise DeploymentRunError(
                node, "Error while rebooting", status, stdout, stderr)

        if not client.waitForReady(initialWait=15, pollfrequency=5):
            raise DeploymentRunError(
                node, "Unable to reboot the system")

        return node

@ClassLogger
class UploadToDirectory(Deployment):
    """
    Upload files to the specified directory and return the full
    remote paths of the files

    :param directory: remote directory
    :type directory: str
    :param fileNames: file names
    :type fileNames: [str]
    """
    def __init__(self, directory, *fileNames):
        super(UploadToDirectory, self).__init__()
        self.directory = directory
        self.fileNames = []
        for fileName in fileNames:
            potentialFileNames = glob.glob(fileName)
            if not potentialFileNames:
                raise ValueError("'{0}' is an invalid path".format(fileName))
            for potentialFileName in potentialFileNames:
                if not os.path.exists(potentialFileName):
                    raise ValueError("'{0}' is an invalid path".format(potentialFileName))
                self.fileNames.append(potentialFileName)
        self.remoteFileNames = []

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
        if not self.fileNames:
            return node
        client.mkdir(self.directory)
        self.remoteFileNames = []
        for fileName in self.fileNames:
            remoteFileName = os.path.join(self.directory, os.path.basename(fileName))
            if client.isFile(remoteFileName):
                self.log.debug("'%s' already uploaded", fileName)
            else:
                if not client.upload(fileName.strip(), remoteFileName):
                    raise DeploymentRunError(node, "Could not upload '{0}'".format(fileName))
                self.remoteFileNames.append(remoteFileName)
        return node

def isRPMPackageInstalled(client, *rpms):
    """
    Determine if the specified rpm packages are installed

    :param client: connected SSH client
    :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
    :param rpms: rpm package names
    :type rpms: [str]
    :returns: mapping of package names to bools
    :rtype: dict
    """
    if not rpms:
        return {}
    packages = [
        # remove rpm extension
        os.path.splitext(os.path.basename(rpm))[0] if rpm.endswith(".rpm") else os.path.basename(rpm)
        for rpm in rpms
    ]
    stdout, _, _ = client.run("rpm -q {0}".format(" ".join(packages)))
    installed = {}
    for line in stdout.splitlines():
        match = re.match(r"package (?P<name>.+) is not installed", line)
        if match:
            installed[match.group("name")] = False
        else:
            installed[line] = True
    return installed
