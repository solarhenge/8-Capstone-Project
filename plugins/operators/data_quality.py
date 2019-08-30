import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 data_quality_pk_tables="",
                 data_quality_fk_tables="",
                 data_quality_pk_columns="",
                 data_quality_fk_columns="",
                 sw_skip_data_quality_checks=False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.data_quality_pk_tables = data_quality_pk_tables
        self.data_quality_fk_tables = data_quality_fk_tables
        self.data_quality_pk_columns = data_quality_pk_columns
        self.data_quality_fk_columns = data_quality_fk_columns
        self.sw_skip_data_quality_checks = sw_skip_data_quality_checks

    def execute(self, context):
        if not self.sw_skip_data_quality_checks:

            conn = PostgresHook(self.postgres_conn_id)

            errors = []
            info = []
            for pk_table in self.data_quality_pk_tables:
                records = conn.get_records(f"select count(*) from {pk_table}")
                if len(records) < 1 or len(records[0]) < 1:
                    errors.append(f"Data quality check failed. {pk_table} returned no results")
                else:
                    num_records = records[0][0]
                    if num_records < 1:
                        errors.append(f"ERROR - Data quality check failed. {pk_table} contains {num_records} records")
                    else:
                        info.append(f"SUCCESS - Data quality check passed. {pk_table} contains {num_records} records")

                if pk_table == "fct_cic":
                    for idx, fk_table in enumerate(self.data_quality_fk_tables):
                        pk_column = self.data_quality_pk_columns[idx]
                        fk_column = self.data_quality_fk_columns[idx]
                        sql = f"select count(distinct a.{pk_column}) from {pk_table} a left outer join {fk_table} b on a.{pk_column} = b.{fk_column} where 1 = 1 and b.{fk_column} is null" 
                        records = conn.get_records(sql)
                        num_records = records[0][0]
                        if num_records > 0:
                            errors.append(f"ERROR - Data quality check failed. {sql} returned {num_records} rows")
                        else:
                            info.append(f"SUCCESS - Data quality check passed. {sql} returned {num_records} rows")

            for i in info:
                logging.info(i)
        
            errors_encountered = False
            for e in errors:
                errors_encountered = True
                logging.info(e)
            if errors_encountered:
                raise ValueError("Data quality check failed.")




