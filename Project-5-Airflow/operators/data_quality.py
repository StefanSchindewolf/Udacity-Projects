from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries as sql_queries
import logging

class DataQualityOperator(BaseOperator):
    """
        Data Quality Check Operator
        
        Runs a range of data quality checks against the specified tables
         1) Counts table entries
         2) Counts NOT NULL entries
            - Searches for columns in table definition which should be "not null"
            - Counts the not null values for each column
        
        How to apply to other columns:
            - Add the table name of a dimension table in <self.dim_tables>
            - Add the table name of a fact table in <self.fact_tables>
            
        How to add a new check:
            - Write one or more SQL statements that perform the check
            - Integrate the SQL in the code below, either in the dimension table
              or fact table loop
        """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dim_tables = ['artists', 'users', 'songs', 'time',]
        self.fact_tables = ['songplays',]

    def execute(self, context):
        # Initialize database hook
        logging.info('Starting Data Quality Checks')
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        logging.info('Postgres Hook {} initialized, starting DATA QUALITY CHECK operation.'.format(self.conn_id))
        
        # CExecute checks for dimension tables
        for table in self.dim_tables:
            logging.info('Starting analysis of dimension table: '.format(table))
            # Get a list of all columns which should not contain NULL values
            notnull_columns = list(redshift_hook.get_records(sql_queries.dqcheck_1_1.format(table)))
            #notnull_columns = notnull_columns[0]
            for col in notnull_columns:
                logging.info('Found columns with NOT NULL constraint: %s', notnull_columns)
                # Count the entries in the column
                col = col[0]
                entries = redshift_hook.get_records(sql_queries.dqcheck_0_1.format(col, table))
                entries = entries[0]
                logging.info('Column has %s entries', entries)
                # Count the NULL values
                counter = redshift_hook.get_records(sql_queries.dqcheck_1_2.format(col, table, col))
                logging.info('.... of which {} are NULL.'.format(table, col, entries, counter))
            logging.info('Data quality checks done for table {}.'.format(table))
        
        # Execute check for fact tables
        for table in self.fact_tables:
            logging.info('Starting analysis of fact table: '.format(table))
            # Get a list of all columns which should not contain NULL values
            notnull_columns = list(redshift_hook.get_records(sql_queries.dqcheck_1_1.format(table)))
            #notnull_columns = notnull_columns[0]
            for col in notnull_columns:
                logging.info('Found columns with NOT NULL constraint: %s', notnull_columns)
                # Count the entries in the column
                col = col[0]
                entries = redshift_hook.get_records(sql_queries.dqcheck_0_1.format(col, table))
                entries = entries[0]
                # Count the NULL values
                counter = redshift_hook.get_records(sql_queries.dqcheck_1_2.format(col, table, col))
                logging.info('Table: {}, Column: {} has {} entries of which {} are NULL.'.format(table, col, entries, counter))
        
        # Done checking data quality
        logging.info('Data Quality checks finished.')