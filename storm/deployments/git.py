"""
Git related deployments
"""
import logging
import os
import re

from c4.utils.logutil import ClassLogger

from ..thunder import (Deployment,
                       DeploymentRunError,
                       RemoteTemporaryDirectory)
from .software import (InstallRPMPackages, isRPMPackageInstalled)


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
    :param tag: tag name
    :type tag: str
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

@ClassLogger
class Install(Deployment):
    """
    Install specified repository

    :param includeDocumentation: include documentation (doc, html, info)
    :type includeDocumentation: bool
    :param includeManPages: include man pages
    :type includeManPages: bool
    :param version: version
    :type version: str
    """
    def __init__(self, includeDocumentation=False, includeManPages=True, version="2.11.0"):
        super(Install, self).__init__()
        self.includeDocumentation = includeDocumentation
        self.includeManPages = includeManPages
        self.version = version

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
        versionParts = self.version.split(".")
        stdout, _, status = client.run("git --version")
        if status == 0:
            installedVersion = re.search(r"(?P<version>\d+\.\d+.+)", stdout).group("version")
            installedVersionParts = installedVersion.split(".")

            if installedVersionParts == versionParts:
                self.log.info("Git version '%s' already installed", installedVersion)
                return node

            if installedVersionParts > versionParts:
                self.log.info("Newer git version '%s' already installed", installedVersion)
                return node

            gitPackageName = "git-{version}".format(version=installedVersion)
            if any(isRPMPackageInstalled(client, gitPackageName).values()):
                raise DeploymentRunError(node, "Found existing git rpm package. Please uninstall first")

        self.log.debug("Installing prerequisite packages")
        prerequisites = [
            "expat-devel",
            "gettext-devel",
            "curl-devel",
            "perl-devel",
            "zlib-devel",
            "openssl-devel"
        ]
        InstallRPMPackages(*prerequisites).run(node, client)

        if self.includeDocumentation:
            self.log.debug("Installing documentation prerequisite packages")
            documenationPrerequisites = [
                "asciidoc",
                "docbook2X",
                "xmlto"
            ]
            InstallRPMPackages(*documenationPrerequisites).run(node, client)

            stdout, stderr, status = client.run("ln -sf /usr/bin/db2x_docbook2texi /usr/bin/docbook2x-texi")
            if status != 0:
                raise DeploymentRunError(node, "Could not create symlink", status=status, stdout=stdout, stderr=stderr)

        with RemoteTemporaryDirectory(client) as tmpDirectory:

            stdout, stderr, status = client.run(
                "wget --directory-prefix {directory} https://www.kernel.org/pub/software/scm/git/git-{version}.tar.gz".format(
                    directory=tmpDirectory,
                    version=self.version
                )
            )
            if status != 0:
                raise DeploymentRunError(node, "Could not download version '{version}'".format(version=self.version), status=status, stdout=stdout, stderr=stderr)

            stdout, stderr, status = client.run("cd {directory} && tar -zxf *.tar.gz --strip 1".format(directory=tmpDirectory))
            if status != 0:
                raise DeploymentRunError(node, "Could not untar build", status=status, stdout=stdout, stderr=stderr)

            stdout, stderr, status = client.run("cd {directory} && make configure".format(directory=tmpDirectory))
            if status != 0:
                raise DeploymentRunError(node, "Could not make configure", status=status, stdout=stdout, stderr=stderr)

            stdout, stderr, status = client.run("cd {directory} && ./configure --prefix=/usr".format(directory=tmpDirectory))
            if status != 0:
                raise DeploymentRunError(node, "Could not configure", status=status, stdout=stdout, stderr=stderr)

            if self.includeDocumentation:
                buildCommand = "cd {directory} && make all doc man info".format(directory=tmpDirectory)
            else:
                buildCommand = "cd {directory} && make all".format(directory=tmpDirectory)
            stdout, stderr, status = client.run(buildCommand)
            if status != 0:
                if re.search(r"docbook2x-texi: command not found", stderr, re.MULTILINE):
                    self.log.warn("Could not build all of the documentation")
                else:
                    raise DeploymentRunError(node, "Could not build git", status=status, stdout=stdout, stderr=stderr)

            if self.includeDocumentation:
                installCommand = "cd {directory} && make install install-doc install-man install-html install-info".format(directory=tmpDirectory)
            else:
                installCommand = "cd {directory} && make install".format(directory=tmpDirectory)
            stdout, stderr, status = client.run(installCommand)
            if status != 0:
                raise DeploymentRunError(node, "Could not install git", status=status, stdout=stdout, stderr=stderr)

            # man pages are part of the documentation so only install if specifically requested and not already installed
            if self.includeManPages and not self.includeDocumentation:
                stdout, stderr, status = client.run(
                    "wget --directory-prefix {directory} https://www.kernel.org/pub/software/scm/git/git-manpages-{version}.tar.gz".format(
                        directory=tmpDirectory,
                        version=self.version
                    )
                )
                if status != 0:
                    raise DeploymentRunError(node, "Could not download man pages for '{version}'".format(version=self.version), status=status, stdout=stdout, stderr=stderr)

                stdout, stderr, status = client.run(
                    "cd {directory} && tar -zxf git-manpages-*.tar.gz -C /usr/local/share/man".format(
                        directory=tmpDirectory
                    )
                )
                if status != 0:
                    raise DeploymentRunError(node, "Could not install man pages for '{version}'".format(version=self.version), status=status, stdout=stdout, stderr=stderr)

        return node
