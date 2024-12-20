from typing import Any, Callable, Collection, Optional, Mapping
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import BranchPythonOperator
# from airflow.utils.trigger_rule import TriggerRule
from airflow.models.taskinstance import Context

