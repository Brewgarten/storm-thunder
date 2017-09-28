"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: storm-thunder
This project is licensed under the MIT License, see LICENSE
"""
import sys

from setuptools import setup, find_packages

import versioneer


needs_pytest = {"pytest", "test", "ptr", "coverage"}.intersection(sys.argv)
pytest_runner = ["pytest-runner"] if needs_pytest else []

setup(
    author = "IBM",
    author_email = "",
    cmdclass=versioneer.get_cmdclass(),
    description = "Framework to perform deployments onto cloud virtual machines",
    entry_points = {
        "console_scripts" : [
            "storm-thunder = storm.thunder.manager:main"
        ]
    },
    install_requires = [
        "apache-libcloud==1.1",
        "hjson",
        "paramiko==2.0"
    ],
    keywords = "python storm cloud deployment",
    license = "MIT",
    name = "storm-thunder",
    packages = find_packages(),
    setup_requires=[] + pytest_runner,
    tests_require=["pytest", "pytest-cov"],
    url = "",
    version = versioneer.get_version(),
)
