from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
import pendulum


with DAG(
    "myetl",
    schedule="@hourly", 
    start_date=pendulum.datetime(2025, 3, 13, tz="Asia/Seoul"),
    default_args={
        "depends_on_past": False,
    },
    max_active_runs=1,
    
) as dag:
    
    start = EmptyOperator(task_id="start") 
    end = EmptyOperator(task_id="end")

    
    make_data = BashOperator(
    task_id="make_data",
    bash_command="bash /home/jiwon/airflow/make_data.sh /home/jiwon/data/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"
    )
     
    def load_p(dis):
        from myetl.load_parquet import load_parquet
        load_parquet(dis)
        
    def agg_c(dis):
        from myetl.agg_csv import agg_csv
        agg_csv(dis)
 
    
    load_data = PythonVirtualenvOperator(
        task_id="load_data",
        python_callable=load_p,
        requirements=[
           "git+https://github.com/jiwon1118/myetl.git@0.1.1"
        ],
        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
    ) 
    
    agg_data = PythonVirtualenvOperator(
        task_id="agg_data",
        python_callable=agg_c,
        requirements=[
           "git+https://github.com/jiwon1118/myetl.git@0.1.1"
        ],
        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
    ) 
    
    
    start >> make_data >> load_data >> agg_data >> end
 

if __name__ == "__main__":
    dag.test()