from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)
from airflow.sensors.filesystem import FileSensor

DAG_ID = "movie_after"

with DAG(
    'movie_after',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['api', 'movie', 'sensor'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/jiwon1118/movie.git@0.5.0"]
    BASE_DIR = f"/home/jiwon/data/{DAG_ID}"
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    check_done = FileSensor(
        task_id="check.done",
        filepath="/home/jiwon/data/movies/done/dailyboxoffice/{{ds_nodash}}/_DONE",
        fs_conn_id="fs_after_movie",
        poke_interval=180,  # 3분마다 체크
        timeout=3600,  # 1시간 후 타임아웃
        mode="reschedule",  # 리소스를 점유하지 않고 절약하는 방식
    )
    
    def fn_gen_meta(base_path, ds_nodash, **kwargs):
        import json
        import os
        from movie.api.after import read_df, fillna_meta, save_gen
        
        PATH = os.path.expanduser("~/data/movies/merge/dailyboxoffice/dt=")
        save_path = f"{base_path}/meta/meta.parquet"
    
        previous_df = read_df(save_path)
        current_df = read_df(f"{PATH}{ds_nodash}")
        
        r_df = fillna_meta(previous_df, current_df)
        
        save_gen(r_df, save_path)
        
        print("ds_nodash--->" + ds_nodash)
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        
    
    gen_meta = PythonVirtualenvOperator(
        task_id='gen.meta',
        python_callable=fn_gen_meta,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
                "base_path": BASE_DIR
        }
    )
        
        
    def fn_gen_movie(base_path, ds_nodash, **kwargs):
        from movie.api.call import save_df
        import pandas as pd

        meta_path = f"{base_path}/meta/meta.parquet"
        meta_df = pd.read_parquet(meta_path)
        current_df = pd.read_parquet(f"/home/jiwon/data/movies/merge/dailyboxoffice/dt={ds_nodash}")
        
        merged_df = current_df.merge(meta_df, on="movieCd", how="left", suffixes=("_A", "_B"))
        merged_df["multiMovieYn"] = merged_df["multiMovieYn_A"].combine_first(merged_df["multiMovieYn_B"])
        merged_df["repNationCd"] = merged_df["repNationCd_A"].combine_first(merged_df["repNationCd_B"])
        
        final_df = merged_df[current_df.columns]
        final_df['dt'] = ds_nodash
        
        save_df(final_df, f'{base_path}/dailyboxoffice', ['dt', 'multiMovieYn', 'repNationCd'])
        
        
    gen_movie = PythonVirtualenvOperator(
        task_id='gen.movie',
        python_callable=fn_gen_movie,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
                "base_path": BASE_DIR
        }
    )
    
    make_done = BashOperator(
        task_id='make.done',
        bash_command="""
        DONE_BASE=/home/jiwon/data/movies/done/dailyboxoffice
        mkdir -p $DONE_BASE/{{ds_nodash}}
        touch $DONE_BASE/{{ds_nodash}}/_DONE
        """,
        env={'BASE_DIR':BASE_DIR},
        append_env = True
    )
    
    start >> check_done >> gen_meta >> gen_movie >> make_done >> end

