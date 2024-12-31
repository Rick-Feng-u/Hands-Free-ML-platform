import sys
from pathlib import Path
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.taskinstance import Context
from typing import Callable, Optional, Mapping, Any, Collection

sys.path.append(str(Path(__file__).resolve().parents[1]))
from global_files.operators.global_operators import (
    CustomPythonOperator,
    CustomTriggerDagOperator
)

from global_files.operators.cluster_operators import (
    CustomCreateDataProcHubCluster,
    CustomDeleteDataProcCluster,
    CustomSubmitPySparkJobOperator,
    ClusterConfig,
    GetSparkFiles
)
try:
    from ml_platform.main_hub.configs.transformation_configs import (TransformationConfigs,)
except:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from main_hub.config.Transformation_config import (TransformationConfigs, )

transformation_config = TransformationConfigs()

class TransformationClusterConfig(ClusterConfig):
    def __init__(self,
                 num_of_workers: int,
                 m_machine_type: str,
                 w_machine_type: str,
                 spark_prop: dict,
                 dp_config: Optional[Any] = TransformationConfigs.DP_CONFIG,
                 **kwargs
                 ):
        super(TransformationClusterConfig, self).__init__(
            num_of_workers=num_of_workers,
            m_machine_type=m_machine_type,
            w_machine_type=w_machine_type,
            spark_prop=spark_prop,
            dp_config=dp_config,
            **kwargs
        )

    def make(self):
        cluster_config = super(TransformationClusterConfig, self).make()
        return cluster_config

class TransformCreateDataProcClusterOperator(CustomTriggerDagOperator):
    def __init__(self,
                 cluster_name: str,
                 cluster_config: TransformationClusterConfig(
                     num_of_workers=transformation_config.num_of_workers,
                     m_machine_type=transformation_config.m_machine_type,
                     w_machine_type=transformation_config.w_machine_type,
                     spark_prop=transformation_config.spark_prop,
                     dp_config=transformation_config.dp_config).make(),
                 **kwargs
                 ):
        super(TransformCreateDataProcClusterOperator, self).__init__(
            cluster_name=cluster_name,
            task_id='{}_spin_up_dataproc_cluster'.format(cluster_name),
            cluster_config=cluster_config,
            retries=transformation_config.retries,
            **kwargs
        )

    def execute(self, context: Context):
        super(TransformCreateDataProcClusterOperator, self).execute(context=context)

class TransformDeleteDataProcClusterOperator(CustomTriggerDagOperator):
    def __init__(self,
                 cluster_name: str,
                 ):
        super(TransformDeleteDataProcClusterOperator, self).__init__(
            cluster_name=cluster_name,
            task_id='{}_delete_dataproc_cluster'.format(cluster_name),
            trigger_rule=TriggerRule.ALL_DONE, # Change to one_fail later
            retries = transformation_config.retries
        )

    def execute(self, context: Context):
        super(TransformDeleteDataProcClusterOperator, self).execute(context=context)

class TransformSubmitPySparkJobOperator(CustomTriggerDagOperator):
    def __init__(self,
                 cluster_name: str,
                 stage:str,
                 dependency_id: str,
                 dag_name:str,
                 **kwargs
                 ):
        spark_files = GetSparkFiles(dags_id = transformation_config.DAGS_ID,
                                    required_files = transformation_config.required_files,)
        super(TransformSubmitPySparkJobOperator, self).__init__(
            cluster_name=cluster_name,
            task_id=dependency_id + '_' + stage +'_submit_pyspark_job',
            spark_job_name = cluster_name + '_' + stage,
            main = spark_files.get_main(),
            pyfiles = spark_files.get_pyfiles(),
            retries=transformation_config.retries,
            # arguments = [] # uncomment if need for spark files
            **kwargs
        )
class TransformPythonOperator(CustomPythonOperator):
    def __init__(self,
                 task_id: str,
                 python_callable: Callable,
                 op_args: Optional[Collection[Any]] = None,
                 op_kwargs: Optional[Mapping[str, Any]] = None,
                 **kwargs
                 ):
        super(TransformPythonOperator. self).__init__(
            task_id=task_id,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            retries=transformation_config.retries
            **kwargs
        )

    def execute(self, context: Context):
        super(TransformPythonOperator, self).execute(context=context)

class TransformTriggerDagOperator(CustomTriggerDagOperator):
    def __init__(self,
                 task_id: str,
                 trigger_dag_id: str,
                 conf: Optional[Mapping[str, str]] = None,
                 **kwargs
                 ):
        super(TransformTriggerDagOperator, self).__init__(
            task_id=task_id,
            trigger_dag_id=trigger_dag_id,
            conf=conf,
            retries=transformation_config.retries,
            **kwargs
        )

    def execute(self, context: Context):
        super(TransformTriggerDagOperator, self).execute(context=context)