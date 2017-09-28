"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE
"""
import logging

from storm.thunder import BaseNodeInfo, NodesInfoMap


log = logging.getLogger(__name__)

class TestBaseNodeInfo(object):

    def test_getNodesByName(self):

        nodesInformation = NodesInfoMap()
        test = BaseNodeInfo("test", "1.2.3.4", password="password")
        nodesInformation.add(test)
        assert len(nodesInformation) == 1
        # attempt to add the same node
        nodesInformation.add(test)
        assert len(nodesInformation) == 1

        nodes = nodesInformation.getNodesByNames(["test"])
        assert len(nodes) == 1
        assert nodes[0].name == test.name
        assert nodes[0].public_ips == test.public_ips
        assert nodes[0].extra == test.extra
        # invalid node name
        assert len(nodesInformation.getNodesByNames(["invalidNode"])) == 0

        # qualified node name
        testDomain = BaseNodeInfo("test.domain", "1.2.3.4", password="password")
        nodesInformation.add(testDomain)
        assert len(nodesInformation) == 2

        nodes = nodesInformation.getNodesByNames(["test"])
        assert len(nodes) == 1
        assert nodes[0].name == test.name
        assert nodes[0].public_ips == test.public_ips
        assert nodes[0].extra == test.extra

        nodes = nodesInformation.getNodesByNames(["test.domain"])
        assert len(nodes) == 1
        assert nodes[0].name == testDomain.name
        assert nodes[0].public_ips == testDomain.public_ips
        assert nodes[0].extra == testDomain.extra

        assert len(nodesInformation.getNodesByNames(["test","test.domain"])) == 2
        assert len(nodesInformation.getNodesByNames(["test.domain", "test"])) == 2
        assert len(nodesInformation.getNodesByNames(["test", "test.domain", "invalidNode"])) == 2

        # long node name
        testNode = BaseNodeInfo("testNode.subdomain.domain", "1.2.3.4", password="password")
        nodesInformation.add(testNode)
        assert len(nodesInformation) == 3

        nodes = nodesInformation.getNodesByNames(["testNode"])
        assert len(nodes) == 1
        assert nodes[0].name == testNode.name
        assert nodes[0].public_ips == testNode.public_ips
        assert nodes[0].extra == testNode.extra

        nodes = nodesInformation.getNodesByNames(["testNode.subdomain.domain"])
        assert len(nodes) == 1
        assert nodes[0].name == testNode.name
        assert nodes[0].public_ips == testNode.public_ips
        assert nodes[0].extra == testNode.extra
