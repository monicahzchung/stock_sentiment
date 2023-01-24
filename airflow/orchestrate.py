import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="stock_sentiment",
    start_date=pendulum.datetime(2023, 1, 18, tz='EST'),
    schedule='55 12 * * 1-5',
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id="deactivate_fork_safety",
        bash_command="export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES"
    )

    cmd = 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 '
    python_script = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/stream.py'

    t2 = BashOperator(
        task_id="start_stream",
        bash_command=cmd + python_script
    )

    t2.set_upstream(t1)