import os
from typing import Any, Optional
from airflow.models.taskinstance import Context
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, ClusterGenerator, \
    DataprocSubmitPySparkJobOperator, DataprocDeleteClusterOperator
from airflow.utils.trigger_rule import TriggerRule

env = os.getenv('ENV', 'exp')

