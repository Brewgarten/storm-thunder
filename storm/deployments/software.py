"""
Software related deployments
"""
import logging
import os
import re

from c4.utils.logutil import ClassLogger

from ..thunder import (Deployment,
                       DeploymentRunError,
                       RemoteTemporaryDirectory)


log = logging.getLogger(__name__)

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

    def run(self, node, client):

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
class DeployRPMs(Deployment):
    """
    Upload and install RPMs

    :param rpms: the paths to rpms
    :type rpms: [str]
    """
    def __init__(self, rpms):
        super(DeployRPMs, self).__init__()
        self.rpms = rpms

    def run(self, node, client):

        with RemoteTemporaryDirectory(client) as tmpDirectory:

            # upload rpms
            uploadedRPMFileNames = []
            for rpm in self.rpms:
                rpmName = os.path.basename(rpm)
                remoteRPMName = os.path.join(tmpDirectory, rpmName)
                uploaded = client.upload(rpm.strip(), remoteRPMName)
                if not uploaded:
                    raise DeploymentRunError(node, "Could not upload {rpm}".format(rpm=rpmName))
                uploadedRPMFileNames.append(remoteRPMName)

            # avoid stale metadata and caches
            stdout, stderr, status = client.run("/usr/bin/yum clean all")
            if status != 0:
                raise DeploymentRunError(
                    node, "Could not clean yum metadata and caches", status, stdout, stderr)

            # install uploaded rpms
            stdout, stderr, status = client.run("/usr/bin/yum localinstall --assumeyes {0}".format(" ".join(uploadedRPMFileNames)))
            if status != 0:
                raise DeploymentRunError(
                    node, "Could not deploy rpms", status, stdout, stderr)

        return node

@ClassLogger
class UpdateKernel(Deployment):
    """
    Class to update kernel on nodes
    """
    def __init__(self):
        super(UpdateKernel, self).__init__()

    def run(self, node, client):

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
