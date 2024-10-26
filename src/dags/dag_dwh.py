import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from py.load_func import loading_data_into_global_metrics

@dag(
    schedule_interval="0 12 * * *",
    start_date=pendulum.parse("2022-10-02"),
    catchup=True,
   )
def dwh_dag():
    loading_data_into_global_metrics_task = PythonOperator(
        task_id="loading_data_into_global_metrics",
        python_callable=loading_data_into_global_metrics,
        op_kwargs={"snap_date": "{{ ds }}"},
    )

    loading_data_into_global_metrics_task

dag_instance = dwh_dag()