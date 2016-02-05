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
            "storm-thunder-deploy = storm.thunder.manager:main"
        ]
    },
    install_requires = [
        "apache-libcloud",
        "c4-utils",
        "paramiko"
    ],
    keywords = "python storm cloud deployment",
    license = "IBM",
    name = "storm-thunder",
    packages = find_packages(),
    setup_requires=[] + pytest_runner,
    tests_require=["pytest", "pytest-cov"],
    url = "",
    version = versioneer.get_version(),
)
