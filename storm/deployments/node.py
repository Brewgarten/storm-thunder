"""
Common deployments and utility functions for node information

"""
import collections
import logging
import re

from c4.utils.logutil import ClassLogger

from ..thunder import (Deployment,
                       DeploymentRunError)


log = logging.getLogger(__name__)

OperatingSystemInformation = collections.namedtuple("OperatingSystemInformation", ["name", "release", "releaseType"])

@ClassLogger
class AddPathsToBashProfile(Deployment):
    """
    Add paths to specified profile

    :param paths: paths
    :type paths: [str]
    :param profilePath: profile path
    :type profilePath: str
    """
    def __init__(self, paths, profilePath="~/.bash_profile"):
        self.profilePath = profilePath
        self.paths = paths

    def run(self, node, client):

        # make sure the paths are valid
        for path in self.paths:
            stdout, stderr, status = client.run("ls {0}".format(path))
            if status != 0:
                raise DeploymentRunError(node, "'{0}' is not a valid path".format(path), status, stdout, stderr)

        # determine profile path and content
        stdout, stderr, status = client.run("echo {0}".format(self.profilePath))
        fullProfilePath = stdout.strip()
        profile = client.read(fullProfilePath)

        # check for PATH and adjust accordingly
        match = re.search(r"(?P<path>PATH=(?P<existingPaths>.*)$)", profile, re.MULTILINE)
        if match:
            existingPathVariable = match.group("path")
            paths = match.group("existingPaths").split(":")
            for newPath in self.paths:
                if newPath in paths:
                    self.log.debug("'%s' already in PATH variable", newPath)
                else:
                    paths.append(newPath)
            newPathVariable = "PATH={0}".format(":".join(paths))
            profile = profile.replace(existingPathVariable, newPathVariable)
        else:
            self.log.debug("Adding PATH to profile")
            profile += "\nPATH=$PATH:{0}\nexport PATH\n".format(":".join(self.paths))
        client.put(fullProfilePath, contents=profile)

        stdout, stderr, status = client.run(". {0}".format(fullProfilePath))
        if status != 0:
            raise DeploymentRunError(node, "Unable to source {0} file".format(fullProfilePath), status, stdout, stderr)

def getKernelRelease(client):
    """
    Get kernel release as a string.

    :param client: connected SSH client
    :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
    :returns: kernel release
    :rtype: str
    """
    stdout, stderr, status = client.run("uname --kernel-release")
    if status != 0:
        log.error(stderr)
        return None
    kernelRelease = stdout.strip()
    log.debug("Kernel release '%s'", kernelRelease)
    return kernelRelease

def getOperatingSystemInformation(client):
    """
    Get operating system information

    :param client: connected SSH client
    :type client: :class:`~libcloud.compute.ssh.BaseSSHClient`
    :returns: operating system information
    :rtype: :class:`~OperatingSystemInformation`
    """
    stdout, stderr, status = client.run("cat /etc/redhat-release")
    if status != 0:
        log.error(stderr)
        return None
    match = re.match(r"(?P<name>.*) release (?P<release>[0-9.]+) \((?P<releaseType>.+)\)", stdout)
    info = OperatingSystemInformation(match.group("name"), match.group("release"), match.group("releaseType"))
    log.debug(info)
    return info
