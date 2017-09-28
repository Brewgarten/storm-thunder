"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE

This library contains JSON helper functionality

Functionality
-------------
"""

import copy
import datetime
import inspect
import json
import logging

import storm.utils.logutil
import storm.utils.util


log = logging.getLogger(__name__)

@storm.utils.logutil.ClassLogger
class JSONSerializable(object):
    """
    Base class that allows child classes inheriting from it to
    serialize to JSON and deserialize from JSON. For example:

    .. code-block:: python

        class Sample(JSONSerializable):

            def __init__(self):
                self.a = "test"
                self.b = 123

        sample = Sample()
        print sample.toJSON()

    will result in

    .. code-block:: python

        {
            "a": "test",
            "b": 123
        }

    In order to allow deserialization we need to include the class information

    .. code-block:: python

        print sample.toJSON(includeClassInfo=True)

    will result in

    .. code-block:: python

        {
            "@class": "module.Sample",
            "a": "test",
            "b": 123
        }

    which can be deserialized

    .. code-block:: python

        sample = Sample.fromJSONFile(jsonFile)

    .. note::

        In case the child class is a more complex object, attributes are to
        be ignored/renamed, etc. overwrite :py:meth:`fromJSONSerializable`
        for loading objects and :py:meth:`toJSONSerializable` for saving objects
    """
    classAttribute = "@class"

    @staticmethod
    def dictHasType(d, o):
        """
        Check whether the dictionary has the same type as the
        specified object or class

        :param d: a dictionary
        :type d: dict
        :param o: object or class
        :returns: ``True`` if the dictionary has the class attribute and it matches the type, ``False`` otherwise
        :rtype: bool
        """
        if JSONSerializable.classAttribute in d:
            moduleName = c4.utils.util.getFullModuleName(o)
            if inspect.isclass(o):
                name = o.__name__
            else:
                name = o.__class__.__name__
            if d[JSONSerializable.classAttribute] == moduleName + "." + name:
                return True
        return False

    @classmethod
    def fromJSON(clazz, jsonString, objectHook=None):
        """
        Load object from the specified JSON string

        :param clazz: the class to deserialize into
        :type clazz: class
        :param jsonString: a JSON string
        :type jsonString: str
        :param objectHook: a function converting a JSON dictionary
            into a dictionary containing Python objects. If ``None``
            then the default :py:meth:`fromJSONSerializable` is used
        :type objectHook: func
        :returns: object instance of the respective class
        """
        if objectHook is None:
            objectHook = clazz.fromJSONSerializable
        return json.loads(jsonString, object_hook=objectHook)

    @classmethod
    def fromJSONFile(clazz, fileName):
        """
        Load object from the specified JSON file

        :param clazz: the class to deserialize into
        :type clazz: class
        :param fileName: a file with the JSON object
        :type fileName: str
        :returns: object instance of the respective class
        """
        jsonFile = open(fileName)
        jsonString = jsonFile.read()
        instance = clazz.fromJSON(jsonString)
        jsonFile.close()
        return instance

    @classmethod
    def fromJSONSerializable(clazz, d):
        """
        Convert a dictionary from JSON into a respective Python
        objects. By default the dictionary is returned as is.

        :param d: the JSON dictionary
        :type d: dict
        :returns: modified dictionary or Python objects
        """
        # check if we have a Python class reference
        if JSONSerializable.classAttribute in d:
            # get class info
            info = d[JSONSerializable.classAttribute].split(".")
            className = info.pop()
            moduleName = ".".join(info)
            #log.debug("Loading class '%s' from module '%s'" % (className, moduleName))

            # load class from module
            module = __import__(moduleName, fromlist=[className])
            clazz = getattr(module, className)

            # check if the class itself or any of its bases except
            # JSONSerializable have fromJSONSerializable method
            def hasFromJSONSerializable(clazz):

                if clazz is JSONSerializable:
                    return False
                if "fromJSONSerializable" in vars(clazz):
                    return True
                bases = clazz.__bases__
                while bases:
                    newBases = set()
                    for base in bases:
                        if base is not JSONSerializable:
                            if "fromJSONSerializable" in vars(base):
                                return True
                            else:
                                newBases.update(base.__bases__)
                    bases = newBases
                return False

            if hasFromJSONSerializable(clazz):
                return clazz.fromJSONSerializable(d)

            # remove class attribute from dictionary
            d.pop(JSONSerializable.classAttribute)

            # create instance based off constructor
            args = inspect.getargspec(clazz.__init__)
            requiredArguments = [None] * (len(args[0]) - 1)
            instance = clazz(*requiredArguments)
            instance.__dict__.update(d)
            return instance
        return d

    def toJSON(self, includeClassInfo=False, pretty=False):
        """
        Convert object to a JSON string

        :param includeClassInfo: include class info in JSON, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        :param pretty: format JSON nicely using indent and sorted keys
        :type pretty: bool
        :returns: str
        """
        class ObjectJSONEncoder(json.JSONEncoder):
            def default(self, instance):
                if hasattr(instance, "toJSONSerializable"):
                    return instance.toJSONSerializable(includeClassInfo)
                return json.JSONEncoder.default(self, instance)

        if pretty:
            jsonString = json.dumps(self, cls=ObjectJSONEncoder, indent=4, sort_keys=True)
        else:
            jsonString = json.dumps(self, cls=ObjectJSONEncoder, separators=(',', ':'))
        return jsonString

    def toJSONFile(self, fileName, includeClassInfo=False, pretty=False):
        """
        Write object to a file as a JSON string

        :param fileName: file name
        :type fileName: str
        :param includeClassInfo: include class info in JSON, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        """
        jsonFile = open(fileName, 'wb')
        jsonString = self.toJSON(includeClassInfo, pretty)
        self.log.debug(jsonString)
        jsonFile.write(jsonString)
        jsonFile.write("\n")
        jsonFile.flush()
        jsonFile.close()

    def toJSONSerializable(self, includeClassInfo=False):
        """
        Convert object to some JSON serializable Python object such as
        str, list, dict, etc.

        :param includeClassInfo: include class info in JSON, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        :returns: JSON serializable Python object
        """
        serializableDict = {}
        for key, value in self.__dict__.items():
            # only add keys with values
            if value is not None:
                if isinstance(value, JSONSerializable):
                    serializableDict[key] = value.toJSONSerializable(includeClassInfo)
                else:
                    serializableDict[key] = copy.deepcopy(value)
        if includeClassInfo:
            moduleName = c4.utils.util.getFullModuleName(self.__class__)
            serializableDict[JSONSerializable.classAttribute] = moduleName + "." + self.__class__.__name__
        return serializableDict

    @property
    def typeAsString(self):
        """
        Fully qualified type as a string
        """
        moduleName = c4.utils.util.getFullModuleName(self.__class__)
        return moduleName + "." + self.__class__.__name__

class Datetime(datetime.datetime, JSONSerializable):
    """
    JSON serializable datetime
    """
    def toJSONSerializable(self, includeClassInfo=False):
        """
        Convert object to some JSON serializable Python object such as
        str, list, dict, etc.

        :param includeClassInfo: include class info in JSON, this
            allows deserialization into the respective Python objects
        :type includeClassInfo: bool
        :returns: JSON serializable Python object
        """
        formattedDateString = self.toISOFormattedString()
        if includeClassInfo:
            serializableDict = {"value": formattedDateString}
            serializableDict[self.classAttribute] = self.typeAsString
            return serializableDict
        else:
            return formattedDateString

    def toISOFormattedString(self):
        """
        Convert datetime into an ISO formatted string

        :rtype: str
        :returns: ISO formatted string
        """
        formattedString = self.isoformat("T")
        # formatting does not automatically add the zeros if there are no microseconds
        if not self.microsecond:
            formattedString += ".000000"
        return formattedString

    @classmethod
    def fromJSONSerializable(cls, d):
        """
        Convert a dictionary from JSON into a respective Python
        objects. By default the dictionary is returned as is.

        :param d: the JSON dictionary
        :type d: dict
        :returns: modified dictionary or Python objects
        """
        return cls.fromISOFormattedString(d["value"])

    @classmethod
    def fromISOFormattedString(cls, formattedString):
        """
        Convert an ISO formatted string into a Datetime object

        :param formattedString: ISO formatted string
        :type formattedString: str
        :returns: Datetime instance
        """
        return cls.strptime(formattedString, "%Y-%m-%dT%H:%M:%S.%f")
