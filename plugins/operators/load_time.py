from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTimeOperator(BaseOperator):

    ui_color = '#F98866'

    template_fields = ("sql",)

    @apply_defaults
    def __init__(self
                ,postgres_conn_id=""
                ,table=""
                ,sw_delete_time=False
                ,sql=""
                ,*args
                ,**kwargs
                ):

        super(LoadTimeOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.sw_delete_time = sw_delete_time
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadTimeOperator begin')

        # connect
        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # do we want to truncate the time?
        self.log.info("yabba dabba doo!")
        if self.sw_delete_time:
            truncate_sql=f"truncate {self.table}"
            self.log.info(truncate_sql)
            conn.run(truncate_sql)

        # render sql statement
        rendered_sql = self.sql.format(**context)
        self.log.info(rendered_sql)

        # run rendered sql statement
        conn.run(rendered_sql)

        self.log.info('LoadTimeOperator end')
