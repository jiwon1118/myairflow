from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum


def print_kwargs(dag, task, data_interval_start, **kwargs):
   ds = data_interval_start.in_tz("Asia/Seoul").format("YYYYMMDDHH")
   msg = f"{dag.dag_id} {task.task_id} {ds} OK / jiwon"
   from myairflow.send_noti import send_noti
   send_noti(msg)


with DAG(
   "seoul",
   schedule="@hourly",  # 매 시간마다 실행
   #start_date=datetime(2025, 3, 10)
   start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul")
) as dag:
    
   start = EmptyOperator(task_id="start") 
    
   b1 = BashOperator(task_id="b_1", 
                     bash_command="""
                        echo "date => `date`"
                        echo "date_interval_start => '{{ data_interval_start }}'"
                        echo "date_interval_end => '{{ data_interval_end }}'"
                        echo "ds => '{{ ds }}'" 
                        echo "ds_nodash => {{ ds_nodash }}"
                        
                        echo "prev_ds => '{{ prev_ds }}'"
                        echo "prev_ds_nodash => '{{ prev_ds_nodash }}'"
                        echo "task_instance => {{task_instance}}" 
                        echo "next_ds => '{{ next_ds }}'"
                        echo "next_ds_nodash => '{{ next_ds_nodash }}'"                 
                                   
                        echo "prev_execution_date => '{{ prev_execution_date }}'"
                        echo "next_execution_date => '{{ next_execution_date }}'"  
                        echo "prev_execution_date_success => '{{ prev_execution_date_success }}'"
                     """)
    
    
   # echo "prev_data_interval_start => '{{ prev_data_interval_start }}'"
   # echo "prev_data_interval_end => '{{ prev_data_interval_end }}'"
   # echo "prev_data_interval_start_success => {{ prev_data_interval_start_success }}"
   # echo "prev_data_interval_end_success => {{ prev_data_interval_end_success }}"
   
    
   b2_1 = BashOperator(task_id="b_2_1", bash_command="echo 2_1")
   b2_2 = BashOperator(task_id="b_2_2", 
                        bash_command="""
                        echo "data_interval_start : {{ data_interval_start.in_tz('Asia/Seoul') }}"
                        """)
   mkdir_cmd = """
      echo "data_interval_start : {{ data_interval_start.in_tz('Asia/Seoul') }}"
      mkdir -p ~/data/seoul/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}
   """

   
   mkdir = BashOperator(task_id="mkdir", 
                        bash_command=mkdir_cmd)
   
   
   end = EmptyOperator(
      task_id="end",
      #dag=dag,
      #trigger_rule="all_success"  # 성공 시 실행
   )

   send_notification = PythonOperator(
      task_id="send_notification",
      python_callable=print_kwargs,
      #dag=dag,
      #trigger_rule="one_failed"  # 실패 시 실행
   )
   
    
   start >> b1 >> [b2_1, b2_2] >> mkdir >> [send_notification,end]
 

if __name__ == "__main__":
    dag.test()
