import sys
from pathlib import Path
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.taskinstance import Context
from typing import Callable, Optional, Mapping, Any, Collection