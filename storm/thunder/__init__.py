from pkgutil import extend_path

from ._version import get_versions
from .base import (BaseNodeInfo,
                   ClusterDeployment,
                   Datetime, Deployment, DeploymentErrorResult, DeploymentResult, DeploymentResults, DeploymentRunError,
                   NodesInfoMap,
                   deploy,
                   getDeployments)
from .client import (AdvancedSSHClient,
                     RemoteTemporaryDirectory)


__path__ = extend_path(__path__, __name__)

__version__ = get_versions()['version']
del get_versions
