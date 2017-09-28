"""
This library contains Hjson helper functionality

See `Hjson <https://hjson.org/>`_ for more information

Functionality
-------------
"""

import copy
import inspect
import logging
import re

import hjson

import c4.utils.logutil
import c4.utils.util


log = logging.getLogger(__name__)

@c4.utils.logutil.ClassLogger
class HjsonSerializable(object):
    """
    Base class that allows child classes inheriting from it to
    serialize to Hjson and deserialize from Hjson. For example:

    .. code-block:: python

        class Sample(HjsonSerializable):

            def __init__(self):
                self.a = "test"
                self.b = 123

        sample = Sample()
        print sample.toHjson()

    will result in

    .. code-block:: python

        a: test
        b: 123

    In order to allow deserialization we need to include the class information

    .. code-block:: python

        print sample.toHjson(includeClassInfo=True)

    will result in

    .. code-block:: python

        @class: module.Sample
        a: test
        b: 123

    which can be deserialized

    .. code-block:: python

        sample = Sample.fromHjsonFile(hjsonFile)

    .. note::

        In case the child class is a more complex object, attributes are to
        be ignored/renamed, etc. overwrite :py:meth:`fromHjsonSerializable`
        for loading objects and :py:meth:`toHjsonSerializable` for saving objects
    """
    classAttribute = "@class"

    @classmethod
    def fromHjson(cls, hjsonString, objectHook=None):
        """
        Load object from the specified Hjson string

        :param cls: the class to deserialize into
        :type cls: class
        :param hjsonString: a Hjson string
        :type hjsonString: str
        :param objectHook: a function converting a Hjson dictionary
            into a dictionary containing Python objects. If ``None``
            then the default :py:meth:`fromHjsonSerializable` is used
        :type objectHook: func
        :returns: object instance of the respective class
        """
        if objectHook is None:
            objectHook = cls.fromHjsonSerializable
        return hjson.loads(hjsonString, object_hook=objectHook)

    @classmethod
    def fromHjsonFile(cls, fileName):
        """
        Load object from the specified Hjson file

        :param cls: the class to deserialize into
        :type cls: class
        :param fileName: a file with the Hjson object
        :type fileName: str
        :returns: object instance of the respective class
        """
        with open(fileName) as hjsonFile:
            hjsonString = hjsonFile.read()
            instance = cls.fromHjson(hjsonString)
            return instance
        return None

    @classmethod
    def fromHjsonSerializable(cls, hjsonDict):
        """
        Convert a dictionary from Hjson into a respective Python
        objects. By default the dictionary is returned as is.

        :param cls: the class to deserialize into
        :type cls: class
        :param hjsonDict: the Hjson dictionary
        :type hjsonDict: dict
        :returns: modified dictionary or Python objects
        """
        # check if we have a Python class reference
        if HjsonSerializable.classAttribute in hjsonDict:
            # get class info
            info = hjsonDict[HjsonSerializable.classAttribute].split(".")
            className = info.pop()
            moduleName = ".".join(info)

            # load class from module
            module = __import__(moduleName, fromlist=[className])
            cls = getattr(module, className)

            # check if the class itself or any of its bases except
            # HjsonSerializable have fromHjsonSerializable method
            def hasFromHjsonSerializable(cls):
                """
                Determine if specified class has a `fromHjsonSerializable` method

                :param cls: the class to check
                :type: cls: class
                :returns: `True` if class has a `fromHjsonSerializable` method, `False` otherwise
                :rtype: bool
                """
                if cls is HjsonSerializable:
                    return False
                if "fromHjsonSerializable" in vars(cls):
                    return True
                bases = cls.__bases__
                while bases:
                    newBases = set()
                    for base in bases:
                        if base is not HjsonSerializable:
                            if "fromHjsonSerializable" in vars(base):
                                return True
                            else:
                                newBases.update(base.__bases__)
                    bases = newBases
                return False

            if hasFromHjsonSerializable(cls):
                return cls.fromHjsonSerializable(hjsonDict)

            # remove class attribute from dictionary
            hjsonDict.pop(HjsonSerializable.classAttribute)

            # create instance based off constructor
            args = inspect.getargspec(cls.__init__)
            requiredArguments = [None] * (len(args[0]) - 1)
            instance = cls(*requiredArguments)
            for key, value in hjsonDict.items():
                if key in instance.__dict__:
                    instance.__dict__[key] = value
                else:
                    cls.log.warn("Ignoring setting '%s' of '%s.%s' to '%s' because '%s' is not a class attribute",
                                 key, moduleName, className, value, key)
            return instance
        return hjsonDict

    def toHjson(self, includeClassInfo=False, pretty=False):
        """
        Convert object to an Hjson string

        :param includeClassInfo: include class info in Hjson, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        :param pretty: format Hjson nicely using indent and sorted keys
        :type pretty: bool
        :returns: str
        """
        class ObjectHjsonEncoder(hjson.encoderH.HjsonEncoder):
            """
            Specific Hjson encoder for embedding serialization and deserialization
            """
            def default(self, instance): #  see: https://bitbucket.org/logilab/pylint/issues/414/false-positive-for-e0202-method-hidden pylint: disable=method-hidden
                if hasattr(instance, "toHjsonSerializable"):
                    return instance.toHjsonSerializable(includeClassInfo=includeClassInfo)
                return hjson.encoderH.HjsonEncoder.default(self, instance)

        if pretty:
            indent = 4
            hjsonString = hjson.dumps(self, cls=ObjectHjsonEncoder, indent=indent, sort_keys=True)
        else:
            indent = 2
            hjsonString = hjson.dumps(self, cls=ObjectHjsonEncoder, indent=indent)
        # remove root {} and indent
        hjsonString = "\n".join([
            "".join(line[indent:])
            for line in hjsonString[1:-1].splitlines()
        ])
        # remove linbreak before starting { for object attributes
        hjsonString = re.sub(r":\s+\{", ": {", hjsonString, flags=re.MULTILINE)
        return hjsonString

    def toHjsonFile(self, fileName, includeClassInfo=False, pretty=False):
        """
        Write object to a file as a Hjson string

        :param fileName: file name
        :type fileName: str
        :param includeClassInfo: include class info in Hjson, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        """
        with open(fileName, "wb") as hjsonFile:
            hjsonString = self.toHjson(includeClassInfo, pretty)
            self.log.debug(hjsonString)
            hjsonFile.write(hjsonString)
            hjsonFile.write("\n")
            hjsonFile.flush()

    def toHjsonSerializable(self, includeClassInfo=False):
        """
        Convert object to some Hjson serializable Python object such as
        str, list, dict, etc.

        :param includeClassInfo: include class info in Hjson, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        :returns: Hjson serializable Python object
        """
        serializableDict = {}
        for key, value in self.__dict__.items():
            # only add keys with values
            if value is not None:
                if isinstance(value, HjsonSerializable):
                    serializableDict[key] = value.toHjsonSerializable(includeClassInfo=includeClassInfo)
                else:
                    serializableDict[key] = copy.deepcopy(value)
        if includeClassInfo:
            serializableDict[HjsonSerializable.classAttribute] = self.typeAsString
        return serializableDict

    @property
    def typeAsString(self):
        """
        Fully qualified type as a string
        """
        moduleName = c4.utils.util.getFullModuleName(self.__class__)
        return moduleName + "." + self.__class__.__name__
