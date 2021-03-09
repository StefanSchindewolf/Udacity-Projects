from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries as sql_queries
import logging
import configparser



class CreateTableOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        """ The Create Tables Operator just creates the tables (stage, dim, fact)
            It does not take specific arguments and should run only if really required
            since it will wipe all tables.
            Returns:    Nothing
            """
        try:
            logging.info('Creating tables in Redshift')
            redshift_hook = PostgresHook("redshift")
            redshift_hook.run(sql_queries.create_tables)
            logging.info = ('Done')
        except Exception as e:
            logging.error('Loading files failed with error: {}'.format(e))
