from datetime import datetime, timedelta

from airflow import DAG
from helpers import SqlQueries

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.operators import (DataQualityOperator
                              ,HasRowsOperator
                              ,LoadDimensionOperator
                              ,LoadFactOperator
                              ,LoadTimeOperator
                              ,StageToRedshiftOperator
                              )

#from avalon_dim_subdag import dim_subdag
from avalon_stg_subdag import stg_subdag

import sql_statements

#Ubuntu 18.04 LTS
#export aws_access_key_id=
#export aws_secret_key=
import os
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_key = os.environ.get('aws_secret_key')
#print(aws_access_key_id)
#print(aws_secret_key)

start_date = datetime.utcnow()- timedelta(minutes=5)

# Switches
sw_delete_data = False
sw_delete_fact = False
sw_delete_time = False
sw_skip_data_quality_checks = False
data_quality_pk_tables = ['fct_cic']
data_quality_fk_tables = ['dim_addr','dim_airline','dim_cnty','dim_port']
data_quality_pk_columns = ['i94addr','airline','i94cit','i94port']
data_quality_fk_columns = ['i94addr','airline','i94cnty','i94port']

default_args = {'owner': 'dwhuser'
               ## monthly
               ,'start_date': datetime(2016, 4, 1)
               ,'end_date': datetime(2016, 4, 1)
               ## daily
               #,'start_date': datetime(2016, 4, 1)
               #,'end_date': datetime(2016, 4, 30)
               ,'depends_on_past': False
               ,'retries': 1
               ,'retry_delay': timedelta(minutes=1)
               ,'email_on_retry': False
               #,'email': ['airflow@example.com']
               #,'email_on_failure': False
               #,'queue': 'bash_queue'
               #,'pool': 'backfill'
               #,'priority_weight': 10
               }

dag = DAG('avalon'
         ,catchup=True
         ,default_args=default_args
         ,description='project: data engineering capstone project'
         #,schedule_interval=timedelta(days=1)
         #,schedule_interval='0 * * * *'
         #,schedule_interval='@daily'
         #,schedule_interval='@hourly'
         ,schedule_interval='@monthly'
         ,max_active_runs=1
         )

begin_execution = DummyOperator(
    task_id='begin_execution',  
    dag=dag
)

create_dim_time = PostgresOperator(
    task_id="create_dim_time",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.create_table_dim_time
)

create_fct_cic = PostgresOperator(
    task_id="create_fct_cic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.create_table_fct_cic
)

dim_addr_task_id = "dim_addr_subdag"
dim_addr_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_addr_task_id,
        "aws_credentials",
        "redshift",
        "dim_addr",
        sql_statements.create_table_dim_addr,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_addr",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_addr_task_id,
    dag=dag,
)

dim_airline_task_id = "dim_airline_subdag"
dim_airline_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_airline_task_id,
        "aws_credentials",
        "redshift",
        "dim_airline",
        sql_statements.create_table_dim_airline,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_airline",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_airline_task_id,
    dag=dag,
)

dim_airport_codes_task_id = "dim_airport_codes_subdag"
dim_airport_codes_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_airport_codes_task_id,
        "aws_credentials",
        "redshift",
        "dim_airport_codes",
        sql_statements.create_table_dim_airport_codes,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_airport_codes",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_airport_codes_task_id,
    dag=dag,
)

dim_cnty_task_id = "dim_cnty_subdag"
dim_cnty_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_cnty_task_id,
        "aws_credentials",
        "redshift",
        "dim_cnty",
        sql_statements.create_table_dim_cnty,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_cnty",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_cnty_task_id,
    dag=dag,
)

dim_gender_task_id = "dim_gender_subdag"
dim_gender_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_gender_task_id,
        "aws_credentials",
        "redshift",
        "dim_gender",
        sql_statements.create_table_dim_gender,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_gender",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_gender_task_id,
    dag=dag,
)

dim_mode_task_id = "dim_mode_subdag"
dim_mode_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_mode_task_id,
        "aws_credentials",
        "redshift",
        "dim_mode",
        sql_statements.create_table_dim_mode,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_mode",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_mode_task_id,
    dag=dag,
)

dim_port_task_id = "dim_port_subdag"
dim_port_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_port_task_id,
        "aws_credentials",
        "redshift",
        "dim_port",
        sql_statements.create_table_dim_port,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_port",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_port_task_id,
    dag=dag,
)

dim_visas_task_id = "dim_visas_subdag"
dim_visas_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_visas_task_id,
        "aws_credentials",
        "redshift",
        "dim_visas",
        sql_statements.create_table_dim_visas,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_visas",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_visas_task_id,
    dag=dag,
)

dim_visatype_task_id = "dim_visatype_subdag"
dim_visatype_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        dim_visatype_task_id,
        "aws_credentials",
        "redshift",
        "dim_visatype",
        sql_statements.create_table_dim_visatype,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="dim_visatype",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=dim_visatype_task_id,
    dag=dag,
)

stg_cic_task_id = "stg_cic_subdag"
stg_cic_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "avalon",
        stg_cic_task_id,
        "aws_credentials",
        "redshift",
        "stg_cic",
        sql_statements.create_table_stg_cic,
        s3_prefix="s3:/",
        s3_bucket="solarhenge",
        s3_key="stg_cic",
        s3_jsonpath_file="auto",
        sw_delete_data=sw_delete_data,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=stg_cic_task_id,
    dag=dag,
)

load_dim_time = LoadTimeOperator(
    task_id='load_dim_time'
    ,dag=dag
    ,postgres_conn_id="redshift"
    ,table="dim_time"
    ,sw_delete_time=sw_delete_time
    ,sql=f"""
        {SqlQueries.insert_into_dim_time}
        WHERE 1 = 1
        AND to_char(to_timestamp(stg_cic.arrdate,'YYYYMMDD')::timestamp without time zone,'YYYY-MM-DD"T"HH24:00:00"+"00:00') > '{{{{ prev_execution_date }}}}'
        AND to_char(to_timestamp(stg_cic.arrdate,'YYYYMMDD')::timestamp without time zone,'YYYY-MM-DD"T"HH24:00:00"+"00:00') < '{{{{ next_execution_date }}}}'
    """
)

load_fct_cic = LoadFactOperator(
    task_id='load_fct_cic'
    ,dag=dag
    ,postgres_conn_id="redshift"
    ,table="fct_cic"
    ,sw_delete_fact=sw_delete_fact
    ,sql=f"""
        {SqlQueries.insert_into_fct_cic}
        WHERE 1 = 1
        AND to_char(to_timestamp(stg_cic.arrdate,'YYYYMMDD')::timestamp without time zone,'YYYY-MM-DD"T"HH24:00:00"+"00:00') > '{{{{ prev_execution_date }}}}'
        AND to_char(to_timestamp(stg_cic.arrdate,'YYYYMMDD')::timestamp without time zone,'YYYY-MM-DD"T"HH24:00:00"+"00:00') < '{{{{ next_execution_date }}}}'
    """
)

run_data_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks'
    ,dag=dag
    ,postgres_conn_id="redshift"
    ,data_quality_pk_tables=data_quality_pk_tables
    ,data_quality_fk_tables=data_quality_fk_tables
    ,data_quality_pk_columns=data_quality_pk_columns
    ,data_quality_fk_columns=data_quality_fk_columns
    ,sw_skip_data_quality_checks=sw_skip_data_quality_checks
)

end_execution = DummyOperator(
    task_id='end_execution',  
    dag=dag
)

begin_execution >> create_dim_time
create_dim_time >> create_fct_cic

create_fct_cic >> dim_addr_subdag_task
create_fct_cic >> dim_airline_subdag_task
create_fct_cic >> dim_airport_codes_subdag_task
create_fct_cic >> dim_cnty_subdag_task
create_fct_cic >> dim_gender_subdag_task
create_fct_cic >> dim_mode_subdag_task
create_fct_cic >> dim_port_subdag_task
create_fct_cic >> dim_visas_subdag_task
create_fct_cic >> dim_visatype_subdag_task
create_fct_cic >> stg_cic_subdag_task

dim_addr_subdag_task >> load_dim_time
dim_airline_subdag_task >> load_dim_time
dim_airport_codes_subdag_task >> load_dim_time
dim_cnty_subdag_task >> load_dim_time
dim_gender_subdag_task >> load_dim_time
dim_mode_subdag_task >> load_dim_time
dim_port_subdag_task >> load_dim_time
dim_visas_subdag_task >> load_dim_time
dim_visatype_subdag_task >> load_dim_time
stg_cic_subdag_task >> load_dim_time

load_dim_time >> load_fct_cic

load_fct_cic >> run_data_quality_checks

run_data_quality_checks >> end_execution