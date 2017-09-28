"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE

This library contains utility functions

Timer
-----

The following example can be used to set up a timer that will call the ``hello``
function after 5 seconds, then wait 10 seconds and call ``hello`` again another
two times for a total of three calls.

.. code-block:: python

    def hello():
        print "hello"
    timer = Timer("ExampleTimer", hello, initial=5, repeat=3, interval=10)
    timer.start()

Stop Flag Process
-----------------

In multiprocess environments it may become necessary for child or other process to
have control over a particular process, especially stopping it. This can be done
using the :class:`~c4.utils.util.StopFlagProcess`. For example, in order to
allow the pool workers in the :class:`~c4.messaging.AsyncMessageServer` to stop
the server one can do the following:

Create a handler class that makes use of a stop flag variable and add it to the message
server as a handler

.. code-block:: python

    class Handler(object):

        def __init__(self):
            self.stopFlag = None

        def handleMessage(self, message):
            if message == "stop":
                self.stopFlag.set()
            else:
                print message

    handler = Handler()
    messageServer = AsyncMessageServer("tcp://127.0.0.1:5000", "MessageServer")
    messageServer.addHandler(handler.handleMessage)


Connect the stop flag of the handler class to the one of the message server

.. code-block:: python

    handler.stopFlag = messageServer.stopFlag

Worker Pool
-----------

Process or worker pools can be used to distribute and parallelize long running task
and computations. We can utilize the :class:`Worker` class effectively to achieve
this.

Create a task queue and a list of workers

.. code-block:: python

    tasks = multiprocessing.JoinableQueue()
    workerProcesses = [Worker(tasks).start() for i in range(10)]

Add tasks to the task queue

.. code-block:: python

    def work(one, two):
        ...perform long running task

    tasks.put((work, [1, 2]))

Stop the worker pool by filling up the task queue with ``None``, then terminating
and joining the worker processes

.. code-block:: python

    for i in range(10):
        tasks.put(None)
    for w in workerProcesses:
        w.terminate()
    for w in workerProcesses:
        w.join()

Functionality
-------------
"""

import collections

import copy
import datetime
import fcntl
import inspect
import logging
import multiprocessing.managers
import os
import pkgutil
import pwd
import re
import signal
import sys
import time
import traceback

import c4.utils.command
import c4.utils.logutil

C4_SYSTEM_MANAGER = "c4.system.manager"
PARAMETER_DOC_REGEX = re.compile(r"\s*:(?P<docType>\w+)\s+(?P<name>\w+):\s+(?P<description>.+)", re.MULTILINE)

log = logging.getLogger(__name__)

@c4.utils.logutil.ClassLogger
class EtcHosts(collections.OrderedDict):
    """
    A representation of the ``/etc/hosts`` file that allows
    management of host to ip resolution
    """

    def add(self, alias, ip, replace=False):
        """
        Add alias to specified ip.

        :param alias: alias/host name
        :type alias: str
        :param ip: ip address
        :type ip: str
        :param replace: replace existing ip entries for the alias with the new ip
        :type replace: bool
        :returns: alias
        :rtype: str
        """
        if ip not in self:
            self[ip] = set()
        if alias in self[ip]:
            self.log.warn("did not add alias '%s' to '%s' because it already exists", alias, ip)
            return None

        if replace:
            # check if alias already used for a different ip
            for existingIp, aliases in self.items():
                if alias in aliases:
                    # remove alias from the existing ip
                    self.log.warn("changing ip from '%s' to '%s' for alias '%s'", existingIp, ip, alias)
                    self[existingIp].remove(alias)
                    if not self[existingIp]:
                        del self[existingIp]

        self[ip].add(alias)
        return alias

    @staticmethod
    def fromString(string):
        """
        Load object from the specified ``/etc/hosts`` compatible string representation

        :param string: ``/etc/hosts`` compatible string representation
        :type string: str
        :returns: etcHosts object
        :rtype: :class:`~EtcHosts`
        """
        etcHosts = EtcHosts()
        for line in string.splitlines():
            # strip comments
            lineWithoutComments = line.split("#")[0]
            if lineWithoutComments.strip():
                entries = lineWithoutComments.split()
                ip = entries.pop(0).strip()
                etcHosts[ip] = set(entries)
        return etcHosts

    def toString(self):
        """
        Get an ``/etc/hosts`` compatible string representation

        :returns: string representation
        :rtype: str
        """
        entries = []
        for ip, aliases in self.items():
            sortedHostnames = sortHostnames(aliases)
            entries.append("{ip} {aliases}".format(ip=ip, aliases=" ".join(sortedHostnames)))
        return "\n".join(entries) + "\n"

@c4.utils.logutil.ClassLogger
class SharedDictWithLock(collections.MutableMapping, dict):
    """
    A dictionary class that can be shared across processes and performs
    automatic locking.
    """

    def __init__(self):
        self.manager = multiprocessing.managers.SyncManager()
        self.manager.start(disableInterruptSignal)
        self.dict = self.manager.dict()
        self.lock = self.manager.RLock()

    def __getitem__(self, key):
        try:
            return self.dict[key]
        except KeyError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        self.lock.acquire()
        self.dict[key] = value
        self.lock.release()

    def __delitem__(self, key):
        self.lock.acquire()
        try:
            del self.dict[key]
        except KeyError:
            raise KeyError(key)
        finally:
            self.lock.release()

    def keys(self):
        """
        Return a copy of the dictionary's list of keys.
        """
        return self.dict.keys()

    def values(self):
        """
        Return a copy of the dictionary's list of values.
        """
        return self.dict.values()

    def __iter__(self):
        raise NotImplementedError("Iterating over a shared dictionary is not supported")

    def __len__(self):
        return len(self.dict)

    def __str__(self, *args, **kwargs):
        return str(self.dict)

@c4.utils.logutil.ClassLogger
class StopFlagProcess(multiprocessing.Process):
    """
    A separate process that can be used to monitor and stop another process using a
    shared `stop flag`.

    :param process: process
    :type process: :class:`multiprocessing.Process`
    """
    def __init__(self, process):
        super(StopFlagProcess, self).__init__(name="{}-StopFlagProcess".format(process.name))
        self.process = process
        self.stopFlag = multiprocessing.Event()

    def run(self):
        """
        The implementation of the stop flag process. In particular we wait on the shared
        `stop flag` and then attempt to terminate the specified process
        """
        try:
            self.stopFlag.wait()
        except EOFError:
            # ignore broken sync manager pipe for the shared event when process is terminated
            pass
        except KeyboardInterrupt:
            # ignore interrupts when process is terminated
            pass
        except SystemExit:
            # ignore system exit events when process is terminated
            pass
        except:
            self.log.error(traceback.format_exc())
        try:
            self.process.terminate()
        except OSError as e:
            if str(e) == "[Errno 3] No such process":
                # ignore when process is already terminated
                pass
            else:
                self.log.error(traceback.format_exc())
        except:
            self.log.error(traceback.format_exc())

@c4.utils.logutil.ClassLogger
class Timer(multiprocessing.Process):
    """
    A timer that can be used to call a function at a specified time as well as
    repeatedly using an interval

    :param name: name of the timer process
    :type name: str
    :param function: a timer function to be called once the timer is reached
    :type function: func
    :param initial: initial wait time before timer function is fired for the first time (in seconds)
    :type initial: float
    :param repeat: how many times the timer function is to be repeated, use ``-1`` for infinite
    :type repeat: int
    :param interval: wait time between repeats (in seconds)
    :type interval: float
    """
    def __init__(self, name, function, initial=0, repeat=0, interval=0):
        super(Timer, self).__init__(name=name)
        self.initial = initial
        self.repeat = repeat
        self.interval = interval
        self.function = function

    def run(self):
        """
        Timer implementation
        """
        try:
            time.sleep(self.initial)
            if self.repeat < 0:
                while True:
                    self.function()
                    time.sleep(self.interval)
            else:
                while self.repeat >= 0:
                    self.function()
                    time.sleep(self.interval)
                    self.repeat -= 1
        except KeyboardInterrupt:
            self.log.debug("Exiting %s", self.name)
        except:
            self.log.debug("Forced exiting %s", self.name)
            self.log.error(traceback.format_exc())

@c4.utils.logutil.ClassLogger
class Worker(multiprocessing.Process):
    """
    A worker process that picks up tasks from the specified task queue

    :param taskQueue: task queue
    :type taskQueue: :class:`~multiprocessing.JoinableQueue`
    :param name: name
    :type name: str

    .. note::

        The task being put on the task queue need to be specified as a tuple
        using the following format: ``(function, [argument, ...])``

    """
    def __init__(self, taskQueue, name=None):
        super(Worker, self).__init__(name=name)
        self.taskQueue = taskQueue

    def run(self):
        """
        Worker implementation
        """
        running = True
        while running:
            task = self.taskQueue.get()
            if task:
                try:
                    (function, arguments) = task
                    function(*arguments)
                except:
                    self.log.debug(traceback.format_exc())
                    self.log.debug(task)
            else:
                running = False
            self.taskQueue.task_done()

    def start(self):
        """
        Start worker

        :returns: :class:`Worker`
        """
        super(Worker, self).start()
        return self

def addressesMatch(baseAddress, *potentialAddresses):
    """
    Check if potential addresses match base address

    :param baseAddress: base address
    :type baseAddress: str
    :param potentialAddresses: potential addresses
    :type potentialAddresses: str
    :returns: bool
    """
    # filter out None
    potentialAddresses = [p for p in potentialAddresses if p is not None]
    base = baseAddress.split("/")

    for potentialAddress in potentialAddresses:

        potential = potentialAddress.split("/")
        if len(base) == len(potential):

            match = True
            parts = zip(base, potential)
            for basePart, potentialPart in parts:
                if basePart != potentialPart:
                    if basePart != "*" and potentialPart != "*":
                        match = False
            if match:
                return True

    return False

def callWithVariableArguments(handler, *arguments, **keyValueArguments):
    """
    Call the handler method or function with a variable number of arguments

    :param handler: handler
    :type handler: method or func
    :param arguments: handler arguments
    :param keyValueArguments: handler key value arguments
    :returns: response
    """
    handlerArgSpec = inspect.getargspec(handler)
    if inspect.ismethod(handler):
        handlerArguments = handlerArgSpec.args[1:]
    elif inspect.isfunction(handler):
        handlerArguments = handlerArgSpec.args
    else:
        log.error("%s needs to be a method or function", handler)
        return

    # retrieve variable argument map for the handler
    handlerArgumentMap, leftOverArguments, leftOverKeywords = getVariableArguments(handler, *arguments, **keyValueArguments)

    # check for missing required arguments
    missingArguments = [
        key
        for key, value in handlerArgumentMap.items()
        if value == "_notset_"
    ]
    if missingArguments:
        for missingArgument in missingArguments:
            log.error("'%s' is missing required argument '%s'",
                      handler.__name__, missingArgument)
        return None

    # add optional keyword arguments
    if handlerArgSpec.keywords:
        handlerArgumentMap.update(leftOverKeywords)

    # add optional variable arguments
    if handlerArgSpec.varargs:
        # retrieve argument values from the map
        handlerArgumentValues = [
            handlerArgumentMap.pop(argumentName)
            for argumentName in handlerArguments
        ]
        # combine argument values with varargs
        combinedArguments = handlerArgumentValues + list(leftOverArguments)
        return handler(*combinedArguments, **handlerArgumentMap)

    return handler(**handlerArgumentMap)

def checkUser(user):
    """
    Check if running as the specified user.

    :param user: user name
    :type user: str
    :returns: True if running as user, False if not
    :rtype: boolean
    """
    euid = os.geteuid()
    entry = pwd.getpwuid(euid)
    return entry.pw_name == user

def confirmPrompt(prompt=None, default="no"):
    """
    Present a confirmation dialog to the user.

    If confirmed do nothing else exit

    :param prompt: prompt message
    :type prompt: str
    :param default: default answer (yes | no)
    :type default: str
    """
    yes = ["y", "ye", "yes"]
    no = ["n", "no"]

    if not prompt:
        prompt = "Proceed?"
    if default == "yes":
        prompt += " [Y/n]: "
    else:
        prompt += " [y/N]: "

        while True:
            try:
                userInput = raw_input(prompt).lower()
            except:
                exit(1)

            if default is not None and not userInput:
                userInput = default
            if userInput in yes:
                return
            elif userInput in no:
                exit(1)

def disableInterruptSignal():
    """
    Set the interrupt signal of the current process to be ignored

    .. note::

        This may be necessary in sub processes, especially pools to
        handle :py:class:`~KeyboardInterrupt` and other exceptions correctly
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def exclusiveWrite(fileName, string, append=True, tries=3, timeout=1):
    """
    Perform an exclusive write to the specified file

    :param fileName: file name
    :type fileName: str
    :param string: string to be written to file
    :type string: str
    :param append: append to or overwrite the contents of the file
    :type append: bool
    :param tries: number of tries to acquire lock
    :type tries: int
    :param timeout: time out between retries (in seconds)
    :type timeout: float
    :raise OSError: if lock could not be acquired within specified tries
    """
    mode = 'w'
    if append:
        mode = 'a'
    with open(fileName, mode) as f:
        while tries > 0:
            try:
                fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                f.write(string)
                fcntl.lockf(f, fcntl.LOCK_UN)
                break
            except IOError:
                pass
            tries -= 1
            time.sleep(timeout)
        if tries < 1:
            raise OSError("Could not acquire lock on '{0}' before timeout".format(fileName))

def getDocumentation(item):
    """
    Get documentation information on the specified item

    :param item: a class, object, function, etc. with a doc string
    :returns: a dictionary with parsed information on description and parameters
    :rtype: dict
    """
    documentation = {
        "parameters": {}
    }
    if item.__doc__:
        documentation["description"] = "\n".join(
            line.strip()
            for line in item.__doc__.strip().splitlines()
            if line and not line.strip().startswith(":")
        )
        for match in PARAMETER_DOC_REGEX.finditer(item.__doc__):
            if match.group("name") not in documentation["parameters"]:
                documentation["parameters"][match.group("name")] = {}

            if match.group("docType") == "param":
                documentation["parameters"][match.group("name")]["description"] = match.group("description")
            if match.group("docType") == "type":
                documentation["parameters"][match.group("name")]["type"] = match.group("description")
    else:
        documentation["description"] = ""

    return documentation

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

def getFullModuleName(o):
    """
    Get the full module name of an object or class

    :param o: an object or class
    :returns: the full module name of the object

    .. note::

        This is necessary because when executing scripts the top level
        module is referenced as ``__main__`` instead of its full name.
        See `PEP 395 -- Qualified Names for Modules <http://legacy.python.org/dev/peps/pep-0395/>`_

    """
    if o.__module__ != "__main__":
        return o.__module__

    parentDirectory, filename = os.path.split(sys.modules["__main__"].__file__)
    moduleName = os.path.splitext(filename)[0]

    while os.path.exists(parentDirectory + "/__init__.py"):

        parentDirectory, directory = os.path.split(parentDirectory)
        moduleName = directory + '.' + moduleName

    return moduleName

def getModuleClasses(module, baseClass=None, includeSubModules=True):
    """
    Get all classes part of the specified module

    :param module: module
    :type module: mod
    :param baseClass: restrict classes by their base class
    :type baseClass: class
    :param includeSubModules: include sub modules in search
    :type includeSubModules: bool
    :returns: [class]
    """
    modules = [module]
    if includeSubModules:
        # load sub modules
        modules.extend(getSubModules(module))

    classes = set()
    for m in modules:
        for _, cls in inspect.getmembers(m, inspect.isclass):
            # make sure object module is a submodule
            if cls.__module__.startswith(module.__name__):
                classes.add(cls)

    if baseClass:
        return [clazz for clazz in classes if issubclass(clazz, baseClass)]
    else:
        return list(classes)

def getPackageData(package, resource):
    """
    Get a resource from a package

    :param package: package name
    :type package: str
    :param resource: resource path / name
    :type resource: str
    :returns: resource content
    :rtype: str

    .. note::

        This is necessary because by default pkgutil.getData does not
        support overlaying packages where the ``__init__.py`` contains
        .. code-block:: python

            from pkgutil import extend_path
            __path__ = extend_path(__path__, __name__)

        See `pkgutil - Package extension utility <https://docs.python.org/2/library/pkgutil.html?highlight=pkgutil#pkgutil.get_data>`_
    """
    loader = pkgutil.get_loader(package)
    if loader is None or not hasattr(loader, "get_data"):
        return None
    mod = sys.modules.get(package) or loader.load_module(package)
    if mod is None or not hasattr(mod, "__file__"):
        return None

    # Modify the resource name to be compatible with the loader.get_data
    # signature - an os.path format "filename" starting with the dirname of
    # the package's __file__
    parts = resource.split('/')
    for path in mod.__path__:
        resourceName = os.path.join(path, *parts)
        try:
            return loader.get_data(resourceName)
        except IOError:
            # pass through and continue looking at next path
            pass
    log.error("Package '%s' does not contain resource '%s'", package, resource)
    return None

def getSubModules(module):
    """
    Recursively find all sub modules of the specified module

    :param module: module
    :type module: mod
    :returns: [mod]
    """
    submodules = []
    if hasattr(module, "__path__"):
        for importer, moduleName, ispkg in pkgutil.iter_modules(module.__path__, module.__name__ + '.'):
            try:
                subModule = __import__(moduleName, fromlist=[moduleName])
                submodules.append(subModule)
                submodules.extend(getSubModules(subModule))
            except ImportError as ie:
                log.error("Cannot import %s: %s", moduleName, ie)

    return submodules

def getVariableArguments(handler, *arguments, **keyValueArguments):
    """
    Retrieve an argument map for the specified handler with the arguments
    and keywords applied as well as the left over arguments and keywords

    :param handler: handler
    :type handler: method or func
    :param arguments: handler arguments
    :param keyValueArguments: handler key value arguments
    :returns: (handlerArgumentMap, leftOverArguments, leftOverKeywords)
    :rtype: tuple
    """
    handlerArgSpec = inspect.getargspec(handler)
    if inspect.ismethod(handler):
        handlerArguments = handlerArgSpec.args[1:]
    elif inspect.isfunction(handler):
        handlerArguments = handlerArgSpec.args
    else:
        log.error("%s needs to be a method or function", handler)
        return

    if handlerArgSpec.defaults is None:
        handlerDefaults = []
    else:
        handlerDefaults = handlerArgSpec.defaults

    lastRequiredArgumentIndex = len(handlerArguments)-len(handlerDefaults)
    requiredHandlerArguments = handlerArguments[:lastRequiredArgumentIndex]
    optionalHandlerArguments = handlerArguments[lastRequiredArgumentIndex:]

    # determine handler arguments
    handlerArgumentMap = {}
    for name in requiredHandlerArguments:
        handlerArgumentMap[name] = "_notset_"
    for index, name in enumerate(optionalHandlerArguments):
        handlerArgumentMap[name] = handlerDefaults[index]

    # set passed in arguments
    for index, value in enumerate(arguments[:len(handlerArguments)]):
        handlerArgumentMap[handlerArguments[index]] = value
    leftOverArguments = list(arguments[len(handlerArguments):])

    # set passed in keyword arguments
    leftOverKeywords = {}
    for name, value in keyValueArguments.items():
        if name in handlerArgumentMap:
            handlerArgumentMap[name] = value
        else:
            leftOverKeywords[name] = value

    return handlerArgumentMap, leftOverArguments, leftOverKeywords

def initWithVariableArguments(cls, **keyValueArguments):
    """
    Create an instance of the class using the variable arguments

    :param cls: class
    :type cls: class
    :param keyValueArguments: init key value arguments
    :returns: instance
    """
    moduleName = getFullModuleName(cls)
    className = moduleName + "." + cls.__name__

    handlerArgSpec = inspect.getargspec(cls.__init__)
    # retrieve variable argument map for the handler
    handlerArgumentMap, _, leftOverKeywords = getVariableArguments(cls.__init__, **keyValueArguments)

    # check for missing required arguments
    missingArguments = [
        key
        for key, value in handlerArgumentMap.items()
        if value == "_notset_"
    ]
    if missingArguments:
        for missingArgument in missingArguments:
            raise ValueError("'{name}' is missing required argument '{argument}'".format(
                name=className, argument=missingArgument))

    # add optional keyword arguments
    if handlerArgSpec.keywords:
        handlerArgumentMap.update(leftOverKeywords)

    # add optional variable arguments
    if handlerArgSpec.varargs:
        # retrieve argument values from the map
        handlerArgumentValues = [
            handlerArgumentMap.pop(argumentName)
            for argumentName in handlerArgSpec.args[1:]
        ]
        # combine argument values with varargs
        varargsValue = leftOverKeywords.pop(handlerArgSpec.varargs)
        combinedArguments = handlerArgumentValues + list(varargsValue)
        instance = cls(*combinedArguments, **handlerArgumentMap)
    else:
        instance = cls(**handlerArgumentMap)

    for key, value in leftOverKeywords.items():
        log.warn("Key '%s' with value '%s' is not a valid parameter for '%s'", key, value, className)

    return instance

def isVirtualMachine():
    """
    Determine if we are in a virtual machine

    :returns: ``True`` if block devices indicate virtual machine, ``False`` otherwise
    :rtype: bool
    """
    try:
        lsblkCommand = ["/bin/lsblk",
                        "--ascii",
                        "--noheadings",
                        "--nodeps",
                        "--output",
                        "name,type"]
        lsblkOutput = c4.utils.command.execute(lsblkCommand, "Could not execute lsblk command")

        # filter out disk names
        diskNames = [info.split()[0].strip()
                     for info in lsblkOutput.splitlines()
                     if info and info.split()[1].strip() == "disk"]

        for diskName in diskNames:
            if diskName.startswith("xvd") or diskName.startswith("vd"):
                return True
        return False

    except Exception as e:
        log.error("Could not determine whether this is a virtual machine or not")
        log.exception(e)
        return False

def killProcessesUsingFileSystem(path):
    """
    Kill processes on the mounted file system containing specified path

    :param path: path
    :type path: str
    """
    try:
        c4.utils.command.execute(["/sbin/fuser", "-c", path])
    except:
        log.debug("No active processes on the mounted file system containing '%s'", path)
        return
    try:
        log.warn("All processes on the mounted file system containing '%s' are being killed", path)
        c4.utils.command.execute(["/sbin/fuser", "-c", "-k", path])
    except Exception as e:
        log.exception(e)

def mergeDictionaries(one, two):
    """
    Merge two dictionaries

    :param one: first dictionary
    :type one: dict
    :param two: second dictionary
    :type two: dict
    :returns: merged dictionary
    :rtype: dict
    """
    if not isinstance(two, dict):
        return copy.deepcopy(two)

    oneKeys = set(one.keys())
    twoKeys = set(two.keys())

    inBothKeys = oneKeys.intersection(twoKeys)
    oneOnlyKeys = oneKeys - inBothKeys
    twoOnlyKeys = twoKeys - inBothKeys

    merged = {}

    # copy all that is in one
    for key in oneOnlyKeys:
        merged[key] = copy.deepcopy(one[key])

    # copy all that is in two
    for key in twoOnlyKeys:
        merged[key] = copy.deepcopy(two[key])

    # check the keys that are the same and copy the new values
    for key in inBothKeys:

        if isinstance(one[key], dict):
            merged[key] = mergeDictionaries(one[key], two[key])
        else:
            merged[key] = copy.deepcopy(two[key])

    return merged

def naturalSortKey(string):
    """
    Convert string into a natural sort key that honors numbers and hyphens

    :param string: string
    :type string: str
    """
    key = []
    partPattern = re.compile("(/)")
    subpartPattern = re.compile("(-)")
    portionPattern = re.compile("(\d+)")
    for part in partPattern.split(string):
        for subpart in subpartPattern.split(part):
            for portion in portionPattern.split(subpart):
                if portion:
                    if portion.isdigit():
                        key.append(int(portion))
                    else:
                        key.append(portion)
    return key

def sortHostnames(hostnames):
    """
    Sort fully qualified hostnames based on their domain hierarchy. Note that aliases and
    non-qualified names will come after the fully qualified ones.

    :param hostnames: hostnames
    :type hostnames: [str]
    :returns: sorted hostnames
    :rtype: [str]
    """
    hostnameParts = []
    aliases = []
    for hostname in hostnames:
        parts = hostname.split(".")
        if len(parts) > 1:
            parts.reverse()
            hostnameParts.append(parts)
        else:
            aliases.append(hostname)

    hostnameParts.sort()
    aliases.sort()

    sortedHostnames = [".".join(reversed(hostnamePart)) for hostnamePart in hostnameParts]
    sortedHostnames.extend(aliases)
    return sortedHostnames

def updateLDAPUri(activeNode=None, useLocalHost=False):
    """
    Updates the LDAP URI in sssd.conf
    :param activeNode: the active node to be set in LDAP URI
    :type activeNode: str
    :param useLocalHost: whether or not to use localhost in place of node name
    :type useLocalHost: bool
    """
    if not activeNode:
        log.error("Active node name not specified.")
        return 1

    log.info("Updating ldap_uri in /etc/sssd/sssd.conf with the new active node %s", activeNode)
    try:
        # Open the file to read
        finDir  = "/etc/sssd"
        finPath = "/etc/sssd/sssd.conf"
        # It's not guaranteed that this funtion will be run by root, so it may not have
        # appropriate permissions to access /etc/sssd.
        ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')
        bkpPath = "/tmp/sssd.conf.bkp."+ ts
        cmd = ['sudo', 'install', '-m', '600', '-o', 'apuser',  finPath, bkpPath]
        c4.utils.command.execute(cmd)

        fin = open(bkpPath, "r")

        # Open the file to write
        foutPath = "/tmp/sssd.conf.out"
        # Delete /tmp/sssd.conf.out if it already exists
        cmd = ['rm', '-f', foutPath]
        c4.utils.command.execute(cmd)
        fout = open(foutPath, 'w+')

        updateThisSection = False

        # Read the input file line by line, and write to the output file along with the updated ldap_uri
        for line in fin:
            if "[domain/local]" in line:
                # We are in the section of the file in which ldap_uri needs to be updated
                updateThisSection = True

            if updateThisSection and line.strip().startswith("ldap_uri"):
                if useLocalHost:
                    activeNode = "localhost"
                log.info("Updating ldap_uri = ldap://%s", activeNode)
                fout.write("ldap_uri = ldap://"+ activeNode + "\n")
                updateThisSection = False
            else:
                fout.write(line)

        fin.close()
        fout.close()
        log.info("Copying /tmp/sssd.conf.out to /etc/sssd/sssd.conf")
        cmd = ['sudo', 'install', '-m', '600', '-o', 'root',  foutPath, finPath]
        c4.utils.command.execute(cmd)

        # Keep a backup of sssd.conf with a timestamp
        cmd = ['sudo', 'install', '-m', '600', '-o', 'root',  bkpPath, finDir]
        c4.utils.command.execute(cmd)

        # Delete /tmp/sssd.conf.out
        cmd = ['rm', '-f', foutPath]
        c4.utils.command.execute(cmd)

        # Delete /tmp/sssd.conf.bkp.XXX
        cmd = ['rm', '-f', bkpPath]
        c4.utils.command.execute(cmd)

    except Exception as e:
        log.exception("Could not update the file /etc/sssd/sssd.conf %s", e)
        return 1

    # Restart sssd service
    try:
        log.info("Restart sssd service")
        cmd = ['sudo', '/sbin/service', 'sssd', 'restart']
        c4.utils.command.execute(cmd)
    except Exception as e:
        log.exception("Could not restart sssd service %s", e)
        return 1

    return 0

def killServicePids(serviceName, processFilter=None):
    """
    Kill all pids associated with the a service

    :param serviceName: Name of service to be killed
    :type serviceName: str
    :param processFilter: If this text is in the process name, it will be killed.
    :type processFilter: str
    """
    pids = []
    stdout, _, rc = c4.utils.command.run("/sbin/service {} status".format(serviceName))
    if rc == 0:
        match = re.search(r".* \(pid(?P<pids>(\s\d+)+)\) is running", stdout)
        if match:
            pids = match.group("pids").split()
            log.info("Killing pids associated with the %s service: %s", serviceName, pids)
            for pid in pids:
                # only kill processes that contain the filter text.
                stdout, _, rc = c4.utils.command.run("/bin/ps -o cmd= {}".format(pid))
                if rc == 0 and (not processFilter or processFilter in stdout):
                    os.kill(int(pid), signal.SIGKILL)
    return pids