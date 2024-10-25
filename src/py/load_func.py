import boto3
import vertica_python
from airflow.models import Variable

AWS_ACCESS_KEY_ID = Variable.get("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = Variable.get("aws_secret_access_key")

conn_info = {
    "host": Variable.get("vertica_host"),
    "port": Variable.get("vertica_port"),
    "user": Variable.get("vertica_user"),
    "password": Variable.get("vertica_password"),
    "autocommit": True,
}

def unloading_from_s3(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(Bucket=bucket, Key=key, Filename=f"/data/{key}")

def loading_currencies_into_stg(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            '''
                COPY STV202406073__STAGING.currencies 
                    (currency_code, currency_code_with, date_update, currency_with_div)
                FROM LOCAL '/data/currencies_history.csv' 
                DELIMITER ',';
            ''',
            buffer_size=65536,
        )
        return cur.fetchall()

def loading_transactions_into_stg(file_num, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            f'''
                COPY STV202406073__STAGING.transactions
                    (operation_id, account_number_from, account_number_to, currency_code,
                    country, status, transaction_type, amount, transaction_dt)
                FROM LOCAL '/data/transactions_batch_{file_num}.csv' 
                DELIMITER ',';
            ''',
            buffer_size=65536,
        )
        return cur.fetchall()
    
def loading_data_into_global_metrics(snap_date, conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        query = open("/lessons/sql/insert_STV202406073__DWH_global_metrics.sql", "r").read()
        query = query.replace('{{ ds }}', snap_date)
        cur.execute(query)
        return cur.fetchall()

def count_files_in_s3(bucket: str, prefix: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].startswith(prefix)]
    return len(files)