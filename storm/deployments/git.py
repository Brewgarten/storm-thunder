"""
Git related deployments
"""
import logging
import re
import os

from c4.utils.logutil import ClassLogger
from ..thunder import (Deployment,
                       DeploymentRunError)


log = logging.getLogger(__name__)

@ClassLogger
class Deploy(Deployment):
    """
    Deploy specified repository

    :param url: repository url
    :type url: str
    :param branch: branch
    :type branch: str
    :param credentialsFile: path to a gitcredentials file
    :type credentialsFile: str
    :param directory: remote directory to deploy into
    :type directory: str
    :param force: remove old directory before deploying
    :type force: bool
    """
    def __init__(self, url, branch="master", credentialsFile=None, directory="~", force=False, tag=None):
        super(Deploy, self).__init__()
        self.branch = branch
        self.directory = directory
        self.force = force
        self.tag = tag
        self.url = url
        self.urlWithCredentials = None
        if credentialsFile:
            with open(os.path.expanduser(credentialsFile)) as credentialsFileHandle:
                credentials = credentialsFileHandle.read()
                protocol, repoUrl = self.url.split("://", 1)
                match = re.search(r"(?P<url>{protocol}://.*@{repoUrl})".format(protocol=protocol, repoUrl=repoUrl), credentials, re.MULTILINE)
                if match:
                    self.urlWithCredentials = match.group("url")

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

        stdout, stderr, status = client.run("echo {0}".format(self.directory))
        if status != 0:
            raise DeploymentRunError(node, "Could not determine base directory", status, stdout, stderr)
        baseDirectory = stdout.strip()

        if client.exists(baseDirectory):
            if self.force:
                _, _, status = client.run("rm -rf {0}".format(baseDirectory))
            else:
                self.log.info("Repository '%s' is already deployed to '%s'", self.url, baseDirectory)
                return node

        branch = self.tag if self.tag else self.branch
        url = self.urlWithCredentials if self.urlWithCredentials else self.url

        stdout, stderr, status = client.run(
            "git clone --branch {branch} --depth 1 {url} {directory}".format(
                branch=branch,
                url=url,
                directory=baseDirectory
            )
        )
        if status != 0:
            errorString = "Could not deploy '{branch}' of repository '{url}' to '{directory}'".format(
                branch=branch,
                url=self.url,
                directory=baseDirectory
            )
            raise DeploymentRunError(node, errorString, status, stdout, stderr)

        # reset the remotes to the url without credentials
        if self.urlWithCredentials:
            stdout, stderr, status = client.run(
                "cd {directory} && git remote set-url origin {url}".format(
                    directory=baseDirectory,
                    url=self.url
                )
            )
            if status != 0:
                raise DeploymentRunError(node, "Could not reset repository remote url to one without credentials", status, stdout, stderr)

        return node
