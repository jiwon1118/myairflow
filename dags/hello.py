from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

# Directed Acyclic Graph -> 마지막 줄로 그래프를 그려줌
with DAG(
    "hello",
    #schedule=timedelta(days=1), # 1일마다 실행
    schedule="0 * * * *",  # 매 분마다 실행행
    #schedule="@hourly",  # 매 시간마다 실행
    start_date=datetime(2025, 3, 10)
 ) as dag:
    
    start = EmptyOperator(task_id="start") 
    end = EmptyOperator(task_id="end")
    
    start >> end
    
