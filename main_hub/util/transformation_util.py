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