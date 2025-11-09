from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

Producer_path = "/opt/airflow/producer.py"
Consumer_path = "/opt/airflow/consumer.py"



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=2),
}

with DAG(
    dag_id = 'retail_kafka',
    default_args = default_args,
    description='A DAG to run kafka producer and consumer',
    start_date = pendulum.datetime(2024, 1, 1, tz="Europe/Berlin"),
    schedule = None,
    catchup =False,
    tags = ['kafka','retail_event']
 
)as dag:

    run_producer_task= BashOperator(
        task_id = 'run_producer',
        bash_command = f"python {Producer_path}"

    )


    run_consumer_task= BashOperator(
        task_id = 'run_consumer',
        bash_command = f"python {Consumer_path}",
        env={
        'AIRFLOW_RUN_ID': 'consumer-group-{{ dag_run.id }}' 
    }
        
    )


    run_producer_task >>  run_consumer_task




