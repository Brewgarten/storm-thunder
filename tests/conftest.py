"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE
"""
import inspect
import logging
import os
import pytest
import tempfile

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

@pytest.fixture(scope="function")
def mockAdvancedSSHClient(monkeypatch):

    from storm.thunder.client import AdvancedSSHClient

    def insertMockMethodNotImplemented(name):
        def mockMethodNotImplemented(self, *args, **kwargs):
            raise NotImplementedError("Missing mock method '{name}'".format(name=name))
        monkeypatch.setattr("storm.thunder.client.AdvancedSSHClient.{name}".format(name=name), mockMethodNotImplemented)

    for name, _ in inspect.getmembers(AdvancedSSHClient, inspect.ismethod):
        if not name.startswith("_") or name == "__init__":
            insertMockMethodNotImplemented(name)

    # add base methods
    def connect(self):
        return True
    monkeypatch.setattr("storm.thunder.client.AdvancedSSHClient.connect", connect)

    def close(self):
        return True
    monkeypatch.setattr("storm.thunder.client.AdvancedSSHClient.close", close)

    def upload(self, filenames, remotefileOrDirname):
        return filenames
    monkeypatch.setattr("storm.thunder.client.AdvancedSSHClient.upload", upload)


@pytest.fixture
def mockPublicKeyFile(monkeypatch, temporaryFile):
    def expanduser(path):
        return temporaryFile.name
    monkeypatch.setattr(os.path, 'expanduser', expanduser)

@pytest.fixture
def temporaryFile(request):
    tmpFile = tempfile.NamedTemporaryFile()
    def removeTemporaryFile():
        tmpFile.close()
    request.addfinalizer(removeTemporaryFile)
    return tmpFile
