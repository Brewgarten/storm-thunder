#!/usr/bin/env python
"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE

Deployment management functionality
"""
import argparse
import inspect
import logging
import os
import re
import sys

from c4.utils.util import (getVariableArguments,
                           initWithVariableArguments, naturalSortKey)

from storm.thunder.base import (DEFAULT_NUMBER_OF_PARALLEL_DEPLOYMENTS,
                                NodesInfoMap,
                                deploy,
                                getDeployments, getDocumentation)
from storm.thunder.configuration import DeploymentInfos


log = logging.getLogger(__name__)

def getArgumentParser():
    """
    Dynamically generate argument parser based on available deployments
    """
    parser = argparse.ArgumentParser(description="A utility to perform deployments on nodes",
                                     formatter_class=argparse.RawTextHelpFormatter)

    deployTypeParser = parser.add_subparsers(dest="deployment", metavar="deployment")

    driverParser = argparse.ArgumentParser(add_help=False)
    driverParser.add_argument("-v", "--verbose",
                              action="count",
                              help="display debug information")
    driverParser.add_argument("--nodes-json",
                              required=True,
                              metavar="nodes.json",
                              type=argparse.FileType("r"),
                              help="nodes information")
    driverParser.add_argument("--usePrivateIps",
                              action="store_true",
                              default=False,
                              help="use private ip to connect to nodes instead of the public one (default value 'False')")

    for deploymentName, deployment in sorted(getDeployments().items()):

        descriptionLines = [
            line.strip()
            for line in deployment.__doc__.strip().splitlines()
            if line and not line.strip().startswith(":")
        ]

        deployParser = deployTypeParser.add_parser(deploymentName, help="\n".join(descriptionLines), parents=[driverParser])

        documentation = getDocumentation(deployment)
        handlerArgumentMap = getVariableArguments(deployment.__init__)[0]
        # add variable argument if specified in constructor
        variableArgument = inspect.getargspec(deployment.__init__).varargs
        if variableArgument:
            handlerArgumentMap[variableArgument] = "_notset_"
        for name, value in sorted(handlerArgumentMap.items()):

            if any([action.dest == name for action in deployParser._actions]):
                log.debug("skipping argument '%s' because it overlaps with one in parent parser", name)
                continue

            argumentProperties = {}

            # check for description
            if name in documentation["parameters"]:
                argumentProperties["help"] = documentation["parameters"][name].get("description")

                # check if we can get information about type
                if "type" in documentation["parameters"][name]:
                    if re.match(r"\[.+\]", documentation["parameters"][name]["type"]):
                        argumentProperties["action"] = "append"
            else:
                log.warn("'%s' documentation is missing information for parameter '%s'", deploymentName, name)

            if value == "_notset_":
                if argumentProperties.get("action") == "append":
                    # if multiple values required for a positional argument prefix it with -- and remove trailing s
                    argumentProperties["dest"] = name
                    name = "--{0}".format(name.strip("s"))
                    argumentProperties["required"] = True
                    if "help" in argumentProperties:
                        argumentProperties["help"] = "\n".join([argumentProperties["help"], "(can be specified multiple times)"])
                    else:
                        argumentProperties["help"] = "(can be specified multiple times)"
            else:
                name = "--{0}".format(name)
                argumentProperties["default"] = value
                if isinstance(value, bool):
                    argumentProperties["action"] = "store_true"
                if "help" in argumentProperties:
                    argumentProperties["help"] = "\n".join([argumentProperties["help"], "(default value '{0}')".format(value)])
                else:
                    argumentProperties["help"] = "(default value '{0}')".format(value)

            deployParser.add_argument(name, **argumentProperties)

        deployParser.add_argument("nodes",
                                  nargs="*",
                                  default=[],
                                  type=str,
                                  help="The names of nodes to deploy onto")

    return parser

def getConfigArgumentParser():
    """
    Configuration file argument parser
    """
    parser = argparse.ArgumentParser(description="A utility to perform deployments on nodes",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--config",
                        type=argparse.FileType("r"),
                        help="deployment configuration")
    parser.add_argument("--nodes-json",
                        required=True,
                        metavar="nodes.json",
                        type=argparse.FileType("r"),
                        help="nodes information")
    parser.add_argument("--parallel",
                        default=DEFAULT_NUMBER_OF_PARALLEL_DEPLOYMENTS,
                        type=int,
                        help="number of deployments to run in parallel (default: {})".format(DEFAULT_NUMBER_OF_PARALLEL_DEPLOYMENTS))
    parser.add_argument("--usePrivateIps",
                        action="store_true",
                        default=False,
                        help="use private ip to connect to nodes instead of the public one (default value 'False')")
    parser.add_argument("-v", "--verbose",
                        action="count",
                        help="display debug information")
    return parser

def getDeploymentSections(deploymentInfos, nodesInformation):
    """
    Combine deployments to the same nodes into lists in order to save on connect/disconnect time

    :param deploymentInfos: deployment infos
    :type: :class:`~storm.thunder.configuration.DeploymentInfos`
    :param nodesInformation: nodes information
    :rtype: :class:`~storm.thunder.NodesInfoMap`
    :returns: deployment sections
    :rtype: [([:class:`~storm.thunder.BaseDeployment`], [:class:`~BaseNodeInfo`])]
    """
    deploymentSections = []
    currentNodes = []
    currentSection = []

    for deploymentInfo in deploymentInfos.deployments:
        if deploymentInfo.nodes:
            deploymentNodes = nodesInformation.getNodesByNames(deploymentInfo.nodes)
        else:
            deploymentNodes = nodesInformation.nodes.values()
        currentNodeNames = set([node.name for node in currentNodes])
        nodeNames = set([node.name for node in deploymentNodes])
        if currentNodeNames == nodeNames:
            currentSection.append(deploymentInfo.deployment)
        else:
            if currentSection:
                deploymentSections.append((currentSection, currentNodes))
            currentNodes = deploymentNodes
            currentSection = [deploymentInfo.deployment]

    if currentSection:
        deploymentSections.append((currentSection, currentNodes))

    return deploymentSections

# TODO: move to utils
def getFilenames(listFile=None, fileNames=None):
    """
    Get a combined list of file names

    :param listFile: an open file containing a list of files
    :type listFile: :class:`~file`
    :param fileNames: file names
    :type fileNames: [str]
    :returns: consolidated list of file names
    :rtype: [str]
    """
    names = fileNames or []
    if listFile:
        for line in listFile:
            fileName = line.strip()
            if fileName:
                names.append(fileName)

    paths = []
    for path in names:
        absolutePath = os.path.abspath(path)
        if not os.path.exists(absolutePath):
            log.error("'%s' is not a valid file name", absolutePath)
        elif os.path.isdir(absolutePath):
            # include all files in the directory
            for subpath in os.listdir(absolutePath):
                paths.append(os.path.join(absolutePath, subpath))
        else:
            paths.append(absolutePath)
    return paths

def main():
    """
    Main function of the cloud deployment tooling setup
    """
    logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

    # check if config specified
    usingConfigFile = False
    if "--config" in sys.argv[1:]:
        # use configuration file parser
        configParser = getConfigArgumentParser()
        args = configParser.parse_args()
        usingConfigFile = True
    else:
        # use cli parser
        parser = getArgumentParser()
        args = parser.parse_args()

    logging.getLogger("storm").setLevel(logging.INFO)
    logging.getLogger("storm.thunder.client.AdvancedSSHClient").setLevel(logging.INFO)
    logging.getLogger("c4.utils").setLevel(logging.INFO)
    logging.getLogger("paramiko").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)

    if args.verbose > 0:
        logging.getLogger("storm").setLevel(logging.DEBUG)
        logging.getLogger("storm.thunder.client.AdvancedSSHClient").setLevel(logging.INFO)
        logging.getLogger("c4.utils").setLevel(logging.INFO)
    if args.verbose > 1:
        logging.getLogger("storm.thunder.client.AdvancedSSHClient").setLevel(logging.DEBUG)
        logging.getLogger("c4.utils").setLevel(logging.DEBUG)
    if args.verbose > 2:
        logging.getLogger("paramiko").setLevel(logging.INFO)
        logging.getLogger("requests").setLevel(logging.INFO)
    if args.verbose > 3:
        logging.getLogger("paramiko").setLevel(logging.DEBUG)
        logging.getLogger("requests").setLevel(logging.DEBUG)

    nodesInformation = NodesInfoMap.fromJSONFile(args.nodes_json.name)

    if not usingConfigFile and args.nodes:
        nodes = nodesInformation.getNodesByNames(args.nodes)
    else:
        nodes = nodesInformation.nodes.values()
    nodes = sorted(nodes, key=lambda node: naturalSortKey(node.name))

    if args.usePrivateIps:
        for node in nodes:
            if not node.private_ips:
                raise ValueError("Node {}{} does not have private IP".format(node.name,
                                                                             '(id={})'.format(node.id) if hasattr(node, 'id') else ''))

    if usingConfigFile:
        config = args.config.read()
        args.config.close()
        if args.config.name.endswith(".json"):
            deploymentInfos = DeploymentInfos.fromJSON(config)
        elif args.config.name.endswith(".storm"):
            deploymentInfos = DeploymentInfos.fromHjson(config)
        else:
            log.error("Unknown config file format, please specify a '.json' or '.storm' file")
            return 1

        for deploymentSection in getDeploymentSections(deploymentInfos, nodesInformation):
            deployments, nodes = deploymentSection
            results = deploy(deployments, nodes, usePrivateIps=args.usePrivateIps, numberOfParallelDeployments=args.parallel)
            if results.numberOfErrors:
                return results.numberOfErrors

        return 0

    else:
        log.info("Deploying on nodes: %s", ",".join(node.name for node in nodes))

        parameters = {
            name: value
            for name, value in args.__dict__.items()
            if not name.startswith("_")
        }

        # remove common options
        usePrivateIps = parameters.pop("usePrivateIps", False)
        fullDeploymentClassName = "storm.deployments.{0}".format(parameters.pop("deployment"))
        parameters.pop("nodes", None)
        parameters.pop("nodes_json", None)
        parameters.pop("verbose", None)

        # get class info
        info = fullDeploymentClassName.split(".")
        className = info.pop()
        moduleName = ".".join(info)

        # load class from module
        module = __import__(moduleName, fromlist=[className])
        deploymentClass = getattr(module, className)
        deployment = initWithVariableArguments(deploymentClass, **parameters)

        # TODO: add argument parser option for results file
        # TODO: add argument parser option for timeout
        results = deploy(deployment, nodes, usePrivateIps=usePrivateIps)

        # TODO: display detailled information on success and errors
        return results.numberOfErrors

if __name__ == '__main__':
    sys.exit(main())

