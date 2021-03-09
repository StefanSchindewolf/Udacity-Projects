from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries as sql_queries
import psycopg2
import logging

class LoadDimensionOperator(BaseOperator):
    """
        Load Dimension Table Operator
        
        The operator will load a given table specified in variable <dim_table>
        and will execute the SQL statement(s) related to that table.
        
        For each table a reference to SqlQueries file is stored in the dict
        "self.tables". Additional tables need to be added here.
        
        Upon calling the operator, the corresponding entry is searched in the
        dictionary and the SQL statement then executed.
        
        Note: Operator can run in "append" or "replace" mode. By appending data
        you will save some time in contrast to reading everything again for this
        dimension (hence SQL starts with "insert into". In replacing mode the
        operator will execute a "create table as" resulting in a new table.
        """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 dim_table,
                 append,
                 conn_id,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.dim_table = dim_table
        self.conn_id = conn_id
        self.append = append
        self.tables = {'songs': sql_queries.song_table_insert, 'users': sql_queries.user_table_insert, 'time': sql_queries.time_table_insert, 'artists': sql_queries.artist_table_insert}
        self.append_mode = {True: '''INSERT INTO {} ''', False: '''CREATE TABLE {} AS ''',}


    def execute(self, context):
        logging.info('Starting Dimension Table operations for table {}'.format(self.dim_table))
        # Initialize hook
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        
         
        # Check append mode
        if self.append is True:
            logging.info('Postgres Hook {} initialized, starting LOAD DIM operation in <Append Mode>.'.format(self.conn_id))
        else:
            logging.info('Postgres Hook {} initialized, starting LOAD DIM operation in <Replace Mode>.'.format(self.conn_id))
      
        # Check if table exists
        exists = redshift_hook.get_records(sql='select distinct(pg_table_def.tablename) from pg_table_def')
        for tpl in exists:
            if tpl[0] in self.dim_table:
                logging.info('Dimension table exists')
                exists = True

        # Count entries if tables exist and prepare load, if NOT then create it
        if exists is True:
            if self.append is True:
                sql_prep = sql_queries.insert_into.format(self.dim_table)
            else:
                sql_prep = sql_queries.create_as.format(self.dim_table,self.dim_table)
                logging.warning('Table exists but <Replace> was chosen, trying to drop table.')
                try:
                    redshift_hook.run('DROP table {};'.format(self.dim_table))
                except Exception as e:
                    logging.error('Load Dimension Operator finished with error: ', e)
        else:
            if self.append is True:
                sql_prep = sql_queries.create_as.format(self.dim_table)
            else:
                logging.warn('Table does not exist, creating it despite <Append Mode>')
                sql_prep = sql_queries.create_as.format(self.dim_table,self.dim_table)
        try:
            # Select the SQL statement which corresponds to the given table name in dict "self.tables"
            statement = self.tables[self.dim_table].format(sql_prep)
            redshift_hook.run(statement)
            entries = redshift_hook.get_records(sql='select count(*) from {}'.format(self.dim_table))
            logging.info('Filling table {} succeeded, tables has now {} entries.'.format(self.dim_table, entries[0]))
            logging.info('Load Dimension Operator finished for table {}'.format(self.dim_table))
        except Exception as e:
            logging.error('Load Dimension Operator finished with error: ', e)
        #entries = redshift_hook.get_records(sql='select count(*) from {}'.format(self.dim_table))
        #logging.info('Dimension Table {} has {} entries.'.format(self.dim_table, entries[0]))
        # Choose first part of the SQL command (Insert into for append, Create table as for replace)
        #fill_mode_sql = self.append_mode[self.append].format(self.dim_table)
        
        #statement = (fill_mode_sql + statement).replace('\n', '').replace('\t', '').strip()
        #logging.info(statement)
        # Create the complete SQL command and run it
        #try:
        #    redshift_hook.run(statement)
        #    entries = redshift_hook.get_records(sql='select count(*) from {}'.format(self.dim_table))
        #    logging.info('Filling table {} succeeded, tables has now {} entries.'.format(self.dim_table, entries[0]))
         #   logging.info('Load Dimension Operator finished for table {}'.format(self.dim_table))
        #except Exception as e:
         #   logging.error('Load Dimension Operator finished with error: ', e)
                
