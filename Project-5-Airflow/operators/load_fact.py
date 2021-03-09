from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries as sql_queries
import logging

class LoadFactOperator(BaseOperator):
    """
        Load Fact Table Operator
        
        Inserts records from staging tables into the fact table.
        The fact table name is defined in <self.fact_table>.
        
        Note: Staging tables are fixed for now but could be also
        taken from the Stage Operator.
        """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 fact_table,
                 conn_id,
                 append=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.fact_table = fact_table
        self.conn_id = conn_id
        self.append = append
        self.append_mode = {True: 'INSERT INTO {} ', False: 'CREATE TABLE {} AS ',}


    def execute(self, context):
        logging.info('Starting Fact Load operations for table {}'.format(self.fact_table))
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        # Check if the table exists
        exists = redshift_hook.get_records(sql='select distinct(pg_table_def.tablename) from pg_table_def')
        # If exists then load it by appending or replacing data
        for tpl in exists:
            if tpl[0] in self.fact_table:
                logging.info('Fact table exists')
                exists = True
        if exists is True:
            try:
                entries = redshift_hook.get_records(sql='select count(*) from {}'.format(self.fact_table))
                logging.info('Fact Table {} has {} entries.'.format(self.fact_table, entries[0]))            # If the table does not exist, then stop here
            except Exception as e:
                logging.warning('Counting entries failed with error: {}'.format(e))
            # Check for append/replace mode and assign the corresponding SQL command
            if self.append == True:
                logging.info('Postgres Hook "{}" initialized, starting LOAD FACT operation in <Append Mode>.'.format(self.conn_id))
                sql_prep = sql_queries.insert_into.format(self.fact_table)
            else:
                logging.info('Postgres Hook {} initialized, starting LOAD FACT operation in <Replace Mode>.'.format(self.conn_id))
                sql_prep = sql_queries.create_as.format(self.fact_table,self.fact_table)
        
            # Here the preceding "Insert" or "Create Table As" is assigned
            #statement = self.append_mode[self.append].format(self.fact_table)
            statement = sql_queries.songplay_table_insert.format(sql_prep)
            redshift_hook.run(statement)
            entries = redshift_hook.get_records(sql='select count(*) from {}'.format(self.fact_table))
            logging.info('Filling table {} succeeded, tables has now {} entries.'.format(self.fact_table, entries[0]))
            logging.info('Load Fact Operator finished for table %s', self.fact_table)
        else:
            logging.warning('Could not load table "{}" because it does not exist'.format(self.fact_table))
            logging.warning('Load Fact Operator finished without loading data!')
                
    

