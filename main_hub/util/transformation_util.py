import json
from datetime import datetime, date
from airflow.models import Variable

try:
    from ml_platform.main_hub.configs.transformation_config import (TransformationConfigs,)
    from ml_platform.main_hub.operators.transformation_operators import (
        TransformSubmitPySparkJobOperator
    )
except ImportError:
    from main_hub.config.transformation_config import (TransformationConfigs,)
    from main_hub.operators.transformation_operators import (
        TransformSubmitPySparkJobOperator
    )

transform_config = TransformationConfigs()

class TransformationUtils:
    def __init__(self):
        pass

    def create_submit_spark_jobs(self, cluster_name: str, dependency_list: list, dependency_id: str, dag_id: str) -> dict:

        spark_job_dict = {}

        for dependency in dependency_list:

            current_spark_job = TransformSubmitPySparkJobOperator(
                cluster_name=cluster_name,
                stage=dependency,
                dependency_id=dependency_id,
                dag_name=dag_id
            )

            if dependency in spark_job_dict.keys():
                spark_job_dict[dependency] = current_spark_job
            else:
                spark_job_dict.update({dependency: current_spark_job})

        return spark_job_dict
