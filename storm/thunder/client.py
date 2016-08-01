"""
Functionality to connect to and manage remote nodes
"""
import inspect
import logging
import os
import types
import socket
import stat
import string
import StringIO
import time
import traceback

from functools import wraps

from libcloud.compute.ssh import ParamikoSSHClient, SSHCommandTimeoutError

log = logging.getLogger(__name__)

# TODO: this is a duplicate since we don't want a dependency here, the whole function should probably be moved into c4.utils
def getFormattedArgumentString(arguments, keyValueArguments):
    """
    Get a formatted version of the argument string such that it can be used
    in representations.

    E.g., action("test", key="value") instead of action(["test"],{"key"="value"}

    :param arguments: arguments
    :type arguments: []
    :param keyValueArguments: key value arguments
    :type keyValueArguments: dict
    :returns: formatted argument string
    :rtype: str
    """
    argumentString = ""
    if arguments:
        argumentList = []
        for argument in arguments:
            if isinstance(argument, (str, unicode)):
                argumentList.append("'{0}'".format(argument))
            else:
                argumentList.append(str(argument))
        argumentString += ",".join(argumentList)

    if keyValueArguments:
        if arguments:
            argumentString += ","
        keyValueArgumentList = []
        for key, value in keyValueArguments.items():
            if isinstance(value, (str, unicode)):
                keyValueArgumentList.append("{0}='{1}'".format(key, value))
            else:
                keyValueArgumentList.append("{0}={1}".format(key, str(value)))
        argumentString += ",".join(keyValueArgumentList)

    if argumentString:
        argumentString = "({0})".format(argumentString)

    return argumentString

def MethodExcecutionLogger(cls):
    """
    Class decorator that creates a per-class logger and logs method calls for an SSH client
    """
    cls.log = logging.getLogger("{0}.{1}".format(cls.__module__, cls.__name__))

    def methodLogger(cls, method):
        """
        Decorator that logs hostname and action
        """
        @wraps(method)
        def wrapper(*args, **kwargs):
            """
            Actual logging decorator
            """
            if cls.log.isEnabledFor(logging.DEBUG):
                instance = args[0]
                cls.log.debug("%s: %s%s", instance.hostname, method.func_name, getFormattedArgumentString(args[1:], kwargs))
            result = method(*args, **kwargs)
            return result
        return wrapper

    def runMethodLogger(cls, method):
        """
        Decorator that logs hostname and remote execution information
        """
        @wraps(method)
        def wrapper(*args, **kwargs):
            """
            Actual logging decorator
            """
            if cls.log.isEnabledFor(logging.DEBUG):
                instance = args[0]
                cls.log.debug("%s: %s%s", instance.hostname, method.func_name, getFormattedArgumentString(args[1:], kwargs))
            stdout, stderr, status = method(*args, **kwargs)
            if cls.log.isEnabledFor(logging.DEBUG):
                instance = args[0]
                if status != 0:
                    cls.log.debug("%s: status: %s", instance.hostname, status)
                if stdout:
                    stdout = "".join(filter(lambda x: x in string.printable, stdout)).strip()
                    if stdout:
                        cls.log.debug("%s: output: %s", instance.hostname, stdout)
                # TRICKY: Python logging uses stderr by default so log this as well
                if stderr:
                    stderr = "".join(filter(lambda x: x in string.printable, stderr)).strip()
                    if stderr:
                        cls.log.debug("%s: error: %s", instance.hostname, stderr)
            return stdout, stderr, status
        return wrapper

    # add method logger to add external methods
    for name, method in inspect.getmembers(cls, inspect.ismethod):
        if not name.startswith("_") and name != "connect":
            setattr(cls, name, methodLogger(cls, method))
        if name.startswith("run"):
            setattr(cls, name, runMethodLogger(cls, method))
    return cls

@MethodExcecutionLogger
class AdvancedSSHClient(ParamikoSSHClient):
    """
    Advanced SSH client that extends functionality of the base Paramiko client
    """
    def __init__(self, hostname, port=22, username='root', password=None,
                 key=None, key_files=None, key_material=None, timeout=None):
        super(AdvancedSSHClient, self).__init__(
            hostname, port, username, password,
            key, key_files, key_material, timeout)

    def chmod(self, path, mode):
        """
        Change the mode (permissions) of a file

        :param path: file path
        :type path: str
        :param mode: permissions
        :type mode: int
        """
        extra = {'_path': path}
        self.logger.debug('chmod', extra=extra)
        sftp = self.client.open_sftp()
        try:
            sftp.chmod(path, mode)
        except Exception as e:
            log.error("Could not chmod '%s' to '%s' on '%s'", path, mode, self.hostname)
            log.error(e)
        finally:
            sftp.close()

    def connect(self):
        """
        Connect

        This is a copy of the `libcloud.compute.ssh.ParamikoSSHClient.connect` method
        with support for proper order of public key and password lookups.

        :returns: connection successful
        :rtype: bool
        """
        conninfo = {'hostname': self.hostname,
                    'port': self.port,
                    'username': self.username,
                    'allow_agent': True,
                    'look_for_keys': True}

        if self.password:
            conninfo['password'] = self.password

        if self.key_files:
            conninfo['key_filename'] = self.key_files

        if self.key_material:
            conninfo['pkey'] = self._get_pkey_object(key=self.key_material)

        if self.timeout:
            conninfo['timeout'] = self.timeout

        extra = {'_hostname': self.hostname, '_port': self.port,
                 '_username': self.username, '_timeout': self.timeout}
        self.logger.debug('Connecting to server', extra=extra)

        self.client.connect(**conninfo)
        return True

    def download(self, remotepath, localpath=None, callback=None):
        """
        Download a remote file to the local host
        :param remotepath: remote file to copy
        :type remotepath: str
        :param localpath: destination path on the local host (including the filename). Default: current directory
        :type localpath: str
        :param callback: optional function that accepts the bytes transferred so far and the total bytes to be transferred
        :type form: func(int, int)
        """

        if remotepath is None:
            raise ValueError("no remote file path specified")
        if localpath is None:
            localpath = os.path.join(os.getcwd(), os.path.basename(remotepath))

        sftp = self.client.open_sftp()
        try:
            sftp.get(remotepath, localpath, callback)
        except IOError, e:
            log.error("Couldn't download '{0}'".format(remotepath))
            log.error(e)
        finally:
            sftp.close()

    def isFile(self, path):
        """
        Check if the file specified by the path is a file

        :param path: file path
        :type path: str
        :returns: bool
        """
        extra = {'_path': path}
        self.logger.debug('Is file', extra=extra)
        sftp = self.client.open_sftp()
        try:
            statInfo = sftp.stat(path)
        except:
            return False
        finally:
            sftp.close()
        return statInfo.st_mode & 0170000 == stat.S_IFREG

    def mkdir(self, path):
        """
        Create directory specified by the path

        :param path: directory path
        :type path: str
        """
        extra = {'_path': path}
        self.logger.debug('Creating directory', extra=extra)
        sftp = self.client.open_sftp()
        if path[0] == "/":
            sftp.chdir("/")
        else:
            # Relative path - start from a home directory (~)
            sftp.chdir('.')

        for part in path.split("/"):
            if part != "":
                try:
                    sftp.mkdir(part)
                except IOError:
                    # so, there doesn't seem to be a way to
                    # catch EEXIST consistently *sigh*
                    pass
                sftp.chdir(part)
        sftp.close()

    def read(self, path):
        """
        Read contents of the file specified by the path

        :param path: file path
        :type path: str
        :returns: str
        """
        extra = {'_path': path}
        self.logger.debug('Downloading file', extra=extra)
        sftp = self.client.open_sftp()
        head, tail = os.path.split(path)
        sftp.chdir(head)
        content = ""
        try:
            with sftp.file(tail, mode="r") as f:
                content = f.read()
        except Exception as e:
            log.error("Could not read '%s' on '%s'", path, self.hostname)
            log.error(e)
        finally:
            sftp.close()
        return content

    def run(self, cmd, timeout=None, pseudoTTY=False):
        """
        Run the specified command

        This is a copy of the `libcloud.compute.ssh.ParamikoSSHClient.run` method
        with additional support for pseudo tty.

        :param cmd: command
        :type cmd: str
        :param timeout: How long to wait (in seconds) for the command to
                        finish (optional).
        :type timeout: float
        :param pseudoTTY: allocate a pseudo tty
        :type pseudoTTY: bool
        """
        extra = {'_cmd': cmd}
        self.logger.debug('Executing command', extra=extra)

        # Use the system default buffer size
        bufsize = -1

        transport = self.client.get_transport()
        chan = transport.open_session()

        start_time = time.time()
        if pseudoTTY:
            chan.get_pty()
        chan.exec_command(cmd)

        stdout = StringIO.StringIO()
        stderr = StringIO.StringIO()

        # Create a stdin file and immediately close it to prevent any
        # interactive script from hanging the process.
        stdin = chan.makefile('wb', bufsize)
        stdin.close()

        # Receive all the output
        # Note #1: This is used instead of chan.makefile approach to prevent
        # buffering issues and hanging if the executed command produces a lot
        # of output.
        #
        # Note #2: If you are going to remove "ready" checks inside the loop
        # you are going to have a bad time. Trying to consume from a channel
        # which is not ready will block for indefinitely.
        exit_status_ready = chan.exit_status_ready()

        while not exit_status_ready:
            current_time = time.time()
            elapsed_time = (current_time - start_time)

            if timeout and (elapsed_time > timeout):
                # TODO: Is this the right way to clean up?
                chan.close()

                raise SSHCommandTimeoutError(cmd=cmd, timeout=timeout)

            if chan.recv_ready():
                data = chan.recv(self.CHUNK_SIZE)

                while data:
                    stdout.write(data)
                    ready = chan.recv_ready()

                    if not ready:
                        break

                    data = chan.recv(self.CHUNK_SIZE)

            if chan.recv_stderr_ready():
                data = chan.recv_stderr(self.CHUNK_SIZE)

                while data:
                    stderr.write(data)
                    ready = chan.recv_stderr_ready()

                    if not ready:
                        break

                    data = chan.recv_stderr(self.CHUNK_SIZE)

            # We need to check the exist status here, because the command could
            # print some output and exit during this sleep bellow.
            exit_status_ready = chan.exit_status_ready()

            if exit_status_ready:
                break

            # Short sleep to prevent busy waiting
            time.sleep(1.5)

        # Receive the exit status code of the command we ran.
        status = chan.recv_exit_status()

        stdout = stdout.getvalue()
        stderr = stderr.getvalue()

        extra = {'_status': status, '_stdout': stdout, '_stderr': stderr}
        self.logger.debug('Command finished', extra=extra)

        return [stdout, stderr, status]

    def touch(self, path):
        """
        Touch file specified by path

        :param path: file path
        :type path: str
        """
        extra = {'_path': path}
        self.logger.debug('touch file', extra=extra)
        sftp = self.client.open_sftp()
        try:
            sftp.stat(path)
        except:
            with sftp.file(path, mode="w"):
                pass
        try:
            sftp.utime(path, None)
        except Exception as e:
            log.error("Could not touch '%s' on '%s'", path, self.hostname)
            log.error(e)
        finally:
            sftp.close()

    def upload(self, filenames, remotefileOrDirname):
        """
        Upload local file specified by the path

        :param filenames: file path(s). Multiple file paths can be specified as a list
        :type filename: str or list
        :param remotefileOrDirname: file path or dir name if multiple files are to be uploaded
        :type remotefileOrDirname: str
        :returns: [ file path(s) that have been successfully uploaded ]
        """
        def putWithConfirmation(sftp, filename, remotefilename):
            try:
                sftp.put(filename, remotefilename, confirm=True)
                return True
            except IOError, e:
                log.error("Couldn't upload {0} : {1}".format(filename, traceback.format_exception_only(IOError, e)))
                return False

        extra = {'_path': filenames}
        self.logger.debug('Uploading file(s)', extra=extra)
        uploaded = []
        with self.client.open_sftp() as sftp:
            if type(filenames) == types.ListType:
                for filename in filenames:
                    remotefilename = os.path.join(remotefileOrDirname, os.path.basename(filename))
                    if putWithConfirmation(sftp, filename, remotefilename):
                        uploaded.append(filename)
            else:
                # filenames is not a list, its a single file to be uploaded
                if putWithConfirmation(sftp, filenames, remotefileOrDirname):
                    uploaded.append(filenames)

        return uploaded

    def waitForReady(self, initialWait=None, pollfrequency=5, timeout=600):
        """
        Wait until the node is ready, that is its network interface is up
        :param initialWait: initial wait time before polling can start
        :type initialWait: int
        :param pollfrequency: polling frequency
        :type pollfrequency: int
        :param timeout: timeout
        :type timeout: int
        :returns True if waiting for a ready state does not time out, else False
        :rtype boolean
        """

        # sleep time should always be a minimum 2 seconds between each poll
        if pollfrequency < 2:
            pollfrequency = 2

        if initialWait:
            time.sleep(initialWait)

        pollfrequency -= 1
        sshSocket = socket.socket()
        sshSocket.settimeout(1)
        start = time.time()
        end = start + timeout
        while time.time() < end:

            try:
                sshSocket.connect((self.hostname, self.port))
                sshSocket.recv(256)
                break
            except:
                time.sleep(pollfrequency)

        sshSocket.close()
        if time.time() > end:
            self.log.error("Waiting for '{0}' timed out after '{1}' seconds".format(self.hostname, timeout))
            return False

        return True


class RemoteTemporaryDirectory(object):
    """
    Create a remote temporary directory context using the specified client

    .. code-block:: python

        with RemoteTemporaryDirectory(<client>) as <variable>:
            <body>

    :param client: connected ssh client
    :type client: :class:`BaseSSHClient`
    :param prefix: directory prefix
    :type prefix: str
    :param delete: delete directory when done
    :type delete: bool
    """
    def __init__(self, client, prefix="storm-thunder", delete=True):
        self.client = client
        self.prefix = prefix
        self.delete = delete

    def __enter__(self):
        # create temporary directory
        stdout, stderr, status = self.client.run("/bin/mktemp --directory -t {prefix}-XXXXXXXX".format(prefix=self.prefix))
        if status != 0:
            raise RuntimeError("Could not create remote temporary directory", status, stdout, stderr)
        self.tmpDirectory = stdout.strip()
        return self.tmpDirectory

    def __exit__(self, exceptionType, exception, traceback):
        if self.delete:
            # remove temporary directory
            self.client.run("/bin/rm -rf {tmpDirectory}".format(tmpDirectory=self.tmpDirectory))

        # re-raise if there was an exception
        if exception:
            return False
        return True
