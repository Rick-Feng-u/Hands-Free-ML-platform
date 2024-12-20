import json
from datetime import datetime, date
from pathlib import Path
from airflow import DAG

try:
    from ml_platform.main_hub.configs.transformation_configs import (TransformationConfigs,)
    from ml_platform.main_hub.operators.Transformation_operators import (
        TransfromCreateDataProcClusterOperator,
        TransfromDeleteDataProcClusterOperator,
        TransfromPythonOperator,
    )
    from ml_platform.main_hub.util.Transformation_util import (TransformationUtils, )

except:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from main_hub.config.Transformation_config import (TransformationConfigs,)
    from main_hub.operators.Transformation_operators import (
        TransfromCreateDataProcClusterOperator,
        TransfromDeleteDataProcClusterOperator,
        TransfromPythonOperator,
    )
    from main_hub.util.Transformation_util import (TransformationUtils,)

transform_config = TransformationConfigs()
transform_utils = TransformationUtils()

def transform_dags(intertval_list: list) -> dict:
    dags_dict = {}
    for current_intertval in intertval_list:
        dag_name = transform_config.DAGS_ID + "_" + current_intertval
        cluster_name = transform_config.CLUSTER_ID + "_" + current_intertval

        with DAG(
            dag_name,
            schedule_interval=None,
            on_failure_callback=None,
            on_success_callback=None,
            default_args=transform_config.default_args.get_config_dict()
        ) as dags:
            print("Loading data dependency dictionary")
            data_dependency_dict = transform_utils.get_data_dependency(current_intertval)
            if not data_dependency_dict:
                try:
                    print("Data dependency dictionary loaded")
                    print("Get current run date")
                    get_run_date = TransfromPythonOperator(
                        task_id="get_run_date",
                        python_callable=transform_utils.get_run_date,
                        provide_context=True,
                    )


                except Exception as e:
                    raise print(f"Issues with data dependency {current_intertval}: {e}")
            else:
                raise print("Unable to get data dependency dictionary")



