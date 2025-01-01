import os
import sys
from pathlib import Path
try:
    from ml_platform.global_files.configs.global_configs import (
        dag_config,
        project_config,
        cluster_config
    )
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from global_files.configs.global_configs import (
        dag_config,
        project_config,
        cluster_config
    )

env = os.getenv('ENV', 'exp')

class TransformationConfig:
    def __init__(self, **kwargs):

        self.weekly_interval_transformhub: str = "30 4 * * 6"
        