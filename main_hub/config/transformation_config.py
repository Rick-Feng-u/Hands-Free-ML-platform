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
        self.dag_config = dag_config(env)
        self.project_config = project_config(env)
        self.cluster_config = cluster_config(env)

        self.weekly_interval: str = "30 4 * * 6"
        self.DAGS_ID = "Main_Transformation_Hub"
        self.CLUSTER_ID = ""
        self.NEXT_DAG_ID = "Model_Hub"

        self.INTERTVAL_LIST = ["Biweekly"]
        self.retries = 2

        self.num_workers = 4
        self.m_machine_type = "n2-standard-4"
        self.w_machine_type = "n2-standard-4"

        self.spark_prop = {"spark:spark.executor.cores": '5',
                            "spark:spark.executor.instances": '29',
                            "spark:spark.executor.memory": '34816m',
                            "yarn:yarn.nodemanager.resource.cpu-vcores": '15',
                            "yarn:yarn.nodemanager.resource.memory-mb": '38298',
                            "yarn:yarn.scheduler.maximum-allocation-mb": '38298'}

        self.required_files = []




        