#!/usr/bin/env python
"""
Deployment management functionality
"""
import argparse
import inspect
import logging
import os
import re
import sys

from c4.utils.util import getVariableArguments
from storm.thunder.base import (NodesInfoMap,
                                deploy,
                                getDeployments,
                                getDocumentation)


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
                log.warn("'%s' documentation is missing information for parameter '%s'", deployment.__name__, name)

            if value == "_notset_":
                if argumentProperties.get("action") == "append":
                    # if multiple values required for a positional argument prefix it with -- and remove trailing s
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
                log.fatal(subpath)
                paths.append(os.path.join(absolutePath, subpath))
        else:
            paths.append(absolutePath)
    return paths

def main():
    """
    Main function of the cloud deployment tooling setup
    """
    logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)

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

    if args.nodes:
        nodes = nodesInformation.getNodesByNames(args.nodes)
    else:
        nodes = nodesInformation.nodes.values()
    nodes = sorted(nodes, key=lambda node: node.name)

    if args.usePrivateIps:
        for node in nodes:
            if not node.private_ips:
                raise ValueError("Node {}{} does not have private IP".format(node.name,
                                                                             '(id={})'.format(node.id) if hasattr(node, 'id') else ''))

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

    # create deployment instance
    variableArgument = inspect.getargspec(deploymentClass.__init__).varargs
    if variableArgument:
        variableArgumentValue = parameters.pop(variableArgument.rstrip("s"))
        deployment = deploymentClass(*variableArgumentValue, **parameters)
    else:
        deployment = deploymentClass(**parameters)

    # TODO: add argument parser option for results file
    # TODO: add argument parser option for timeout
    results = deploy(deployment, nodes, usePrivateIps=usePrivateIps)

    # TODO: display detailled information on success and errors
    return results.numberOfErrors

if __name__ == '__main__':
    sys.exit(main())

