from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

# Directed Acyclic Graph -> 마지막 줄로 그래프를 그려줌
with DAG(
    "catchup",
    schedule="@hourly",  # 매 시간마다 실행
    #start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 10, tz="Asia/Seoul"),
    catchup=False
 ) as dag:
    
    start = EmptyOperator(task_id="start") 
    end = EmptyOperator(task_id="end")
    
    start >> end
    