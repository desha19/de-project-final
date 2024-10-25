import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from py.load_func import unloading_from_s3, loading_currencies_into_stg, loading_transactions_into_stg, count_files_in_s3

@dag(
    schedule="0 12 1 * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=False,
)
def transfer_to_stg():
    """Define the Airflow DAG for staging data."""
    download_currencies_task = PythonOperator(
        task_id="download_currencies",
        python_callable=unloading_from_s3,
        op_kwargs={"bucket": "final-project", "key": "currencies_history.csv"},
    )

    load_currencies_task = PythonOperator(
        task_id="loading_currencies_into_stg",
        python_callable=loading_currencies_into_stg,
    )

    # Получаем количество файлов        
    num_files = count_files_in_s3(bucket="final-project", prefix="transactions_batch_")

    download_transactions_tasks = []
    load_transactions_tasks = []
    
    for i in range(1, num_files + 1):
        download_task = PythonOperator(
            task_id=f"download_transactions_{i}",
            python_callable=unloading_from_s3,
            op_kwargs={"bucket": "final-project", "key": f"transactions_batch_{i}.csv"},
        )
        download_transactions_tasks.append(download_task)

        load_task = PythonOperator(
            task_id=f"loading_transactions_into_stg_{i}",
            python_callable=loading_transactions_into_stg,
            op_kwargs={"file_num": i},
        )
        load_transactions_tasks.append(load_task)

    (download_currencies_task >> download_transactions_tasks >> load_currencies_task >> load_transactions_tasks)

dag_instance = transfer_to_stg()