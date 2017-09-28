"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE

This library contains logging helper functionality

Class logging
-------------

By importing the ClassLogger decorator a log attribute is added to the class which
can then be used to log on a per-class basis.

.. code-block:: python

    from c4.utils.logutil import ClassLogger

    @ClassLogger
    class Example:

        def doSomething():
            self.log.debug("something")

Functionality
-------------
"""

import logging

def ClassLogger(cls):
    """
    Class decorator that creates a per-class logger
    """
    cls.log = logging.getLogger("{0}.{1}".format(cls.__module__, cls.__name__))
    return cls
