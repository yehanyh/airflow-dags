import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# 定义默认参数
default_args = {
    'owner': 'airflow',              # DAG 的所有者
    'start_date': days_ago(1),       # DAG 的开始时间（1 天前）
    'retries': 1,                    # 任务失败时的重试次数
    'retry_delay': timedelta(minutes=5),  # 重试间隔
}

# 定义 DAG 对象
with DAG(
    dag_id='hello_world_dag',        # DAG 的唯一标识符
    default_args=default_args,       # 使用默认参数
    schedule_interval='@daily',      # 每天运行一次
    catchup=False,                   # 是否补跑历史任务
) as dag:

    # 定义第一个任务：打印 "Hello"
    def print_hello():
        print("Hello")

    task_hello = PythonOperator(
        task_id='print_hello',        # 任务的唯一标识符
        python_callable=print_hello,  # 调用的 Python 函数
    )

    # 定义第二个任务：打印 "World"
    def print_world():
        print("World")

    task_world = PythonOperator(
        task_id='print_world',
        python_callable=print_world,
    )


    # 定义一个休眠任务
    def sleep_task():
        print("Task is sleeping for 300 seconds...")
        time.sleep(300)  # 休眠 300 秒
        print("Task woke up!")


    sleep_operator = PythonOperator(
        task_id='sleep_task',
        python_callable=sleep_task,
    )

    # 设置任务依赖关系
    task_hello >> task_world >> sleep_operator