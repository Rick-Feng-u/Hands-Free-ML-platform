import json
from datetime import datetime, date
from pathlib import Path
from airflow import DAG
from airflow.utils.task_group import TaskGroup

try:
    from ml_platform.main_hub.configs.transformation_configs import (TransformationConfigs,)
    from ml_platform.main_hub.operators.Transformation_operators import (
        TransfromCreateDataProcClusterOperator,
        TransfromDeleteDataProcClusterOperator,
        TransfromPythonOperator,
        TransfromTriggerNextDag,
    )
    from ml_platform.main_hub.util.Transformation_util import (TransformationUtils, )

except:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from main_hub.config.Transformation_config import (TransformationConfigs,)
    from main_hub.operators.Transformation_operators import (
        TransfromCreateDataProcClusterOperator,
        TransfromDeleteDataProcClusterOperator,
        TransfromPythonOperator,
        TransfromTriggerNextDag,
    )
    from main_hub.util.Transformation_util import (TransformationUtils,)

transform_config = TransformationConfigs()
transform_utils = TransformationUtils()

def transform_dags(intertval_list: list) -> dict:
    """
    This transformation DAG is used to perform data manipulation on raw data
    to create model ready features for model hub later to inference or evaluate later
    """
    dags_dict = {}
    for current_intertval in intertval_list:
        dag_name = transform_config.DAGS_ID + "_" + current_intertval
        base_cluster_name = transform_config.CLUSTER_ID + "_" + current_intertval
        next_dag_name = transform_config.NEXT_DAG_ID

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
                    print("Got current run date")

                    trigger_next_dag = TransfromTriggerNextDag(
                        task_id=f"trigger_{next_dag_name}",
                        trigger_dag_id=next_dag_name,
                    )

                    try:
                        num_for_serial_cluster = 1

                        for data_dependency_id in data_dependency_dict.keys():
                            data_dependency_list = data_dependency_dict[data_dependency_id]
                            # For example Transformation_
                            cluster_name = base_cluster_name + "_clust_" + str(num_for_serial_cluster)
                            strongly_connected_task_group_id = data_dependency_id.upper()

                            with TaskGroup(group_id=strongly_connected_task_group_id):
                                num_for_serial_cluster += 1

                                spin_up_cluster = (
                                    TransfromCreateDataProcClusterOperator(cluster_name=cluster_name)
                                )

                                submit_spark_jobs = transform_utils.create_submit_spark_jobs(
                                    cluster_name=cluster_name,
                                    dependency_list=data_dependency_list,
                                    dependency_id=data_dependency_id,
                                    dag_id=dag_name,
                                )

                                shut_done_cluster = (
                                    TransfromDeleteDataProcClusterOperator(cluster_name=cluster_name)
                                )

                                spark_jobs_sorted_list = None
                                # General idea is spin_up >> [all stages} >> shut-down
                                try:
                                    # since stages have dependency, this need to be sorted(stage1 >> stage2)
                                    spark_jobs_sorted_list = sorted(submit_spark_jobs.keys())

                                    spin_up_cluster >> submit_spark_jobs[spark_jobs_sorted_list[0]]

                                    if len(spark_jobs_sorted_list) > 1:
                                        for i in range(len(spark_jobs_sorted_list) - 1):
                                            submit_spark_jobs[spark_jobs_sorted_list[i]] >> submit_spark_jobs[
                                                spark_jobs_sorted_list[i + 1]]

                                except Exception as e:
                                    raise print(f"Unable to create and/or submit spark jobs: {e}")

                                submit_spark_jobs[spark_jobs_sorted_list[-1]] >> shut_done_cluster

                                submit_spark_jobs[spark_jobs_sorted_list[-1]] >> trigger_next_dag

                    except Exception as e:
                        raise print(f"Unable to create stages: {e}")


                except Exception as e:
                    raise print(f"Issues with data dependency {current_intertval}: {e}")

            else:
                raise print("Unable to get data dependency dictionary")

        dags_dict[dag_name] = dags

    return dags_dict

dags = transform_dags(intertval_list = transform_config.INTERTVAL_LIST)

main_hub_biweekly = dags["main_hub_biweekly"]




