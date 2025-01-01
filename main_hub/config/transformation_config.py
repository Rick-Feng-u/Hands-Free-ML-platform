import os
try:
    from ml_platform
from global_files.configs.global_configs import

env = os.getenv('ENV', 'exp')

class TransformationConfig:
    def __init__(self, **kwargs):

        self.weekly_interval_transformhub: str = "30 4 * * 6"
        