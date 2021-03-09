from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
#from airflow.providers.amazon.aws.hooks.base_aws import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries as sql_queries
from datetime import datetime
import logging
import configparser



class StageToRedshiftOperator(BaseOperator):
    """ 
        Stage Operator which copies JSON files stored on an S3
        location into a table in Redshift using the COPY command.
        
        Specify S3 Bucket with <s3loc> and add a prefix in variable
        <s3pref> (note: s3prefs should have a leading and closing "/"
        while s3loc has no closing "/").
        Specify staging table name with <target_table> and a Redshift
        connection using parameter <conn_id>.
        
        In Airflow, create/update the connections for S3 (here "aws_default")
        and Redshift (here "redshift").
        
        NOTE: if an execdate specified which is previous to the 
        default start_date the operator will create a copy command
        only for the given execution date (to backfill historic data).
        """
    
    template_fields = ['prefix', 'execdate',]
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 key,
                 secret,
                 s3loc,
                 target_table,
                 conn_id,
                 prefix,
                 execdate,
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.key = key
        self.secret = secret
        self.S3_BUCKET = '{}'.format(s3loc)
        self.prefix = prefix
        self.TABLE = target_table
        self.conn_id = conn_id
        self.execdate = execdate

    def execute(self, context):
        """ S3 STAGING FUNCTION
            Works for given S3 Locations and target tables (credentials to AWS
            and Redshift must be provided) by creating a Redshift COPY command
            from <S3 Location> to <target_table>.
            Based on keyword "log" the execute function will import events.
            If the execution date is before the start date, the function will
            just import the events from that specific day.
            Based on keyword "song" the execute function will import songs.
        """
        # If <s3pref> is not assigned, just add a "/" to the S3 Bucket link.
        if self.prefix is None:
            loc = '{}/'.format(self.S3_BUCKET)
        # If there is an <s3pref> we concatenate it with the S3 Bucket link.
        else:
            loc = '{}{}'.format(self.S3_BUCKET, self.prefix)
        logging.info('Searching in path: {}'.format(loc))
        
        # Define the required AWS and Postgres Hoos
        try:
            # Create AWS and Postgres connections, get AWS role ARN
            logging.info(('Starting import from {} into table {} started.').format(self.S3_BUCKET, self.TABLE))
            s3 = AwsHook(aws_conn_id='aws_default')
            #logging.info(s3.get_session())
            role_arn = s3.expand_role('dwhRole')
            logging.info('AWS Hook initialized, using IAM role: {}'.format(role_arn))
            redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
            logging.info('Postgres Hook {} initialized, starting COPY operation.'.format(self.conn_id))
        except Exception as e:
            logging.error('Error when initializing hooks: %s', e)
        
        # If the S3 string contains "log" we want to copy events
        if 'log' in self.prefix:
            try:
                # Get the execution date and start date
                edate = datetime.strptime(self.execdate[:10], '%Y-%m-%d')
                sdate = self.start_date.to_date_string()
                sdate = datetime(int(sdate[:4]), int(sdate[6:7]), int(sdate[9:10]))
                # If execution date is before start date (default) then replace the <loc> string with a
                # Regexp which contains the specified date
                if edate < sdate:
                    logging.info('Execution date {} is before start date {}, switching to Single Load'.format(edate, sdate))
                    loc = '{}{}/{:02}/{}*.json'.format(loc, edate.year, edate.month, edate.date())
                logging.info('Starting import from location: {}'.format(loc))
                redshift_hook.run(sql_queries.staging_events_copy.format(self.TABLE, loc, role_arn))
                #redshift_hook.run(sql_queries.staging_events_copy_key.format(self.TABLE, loc, self.key, self.secret))
                logging.info('Import job for events done')
            except Exception as e:
                logging.error('Loading files failed with error: {}'.format(e))
            
        # If the S3 string contains "song" then we want to copy songs
        elif 'song' in self.prefix:
            try:
                logging.info('Starting import of songs from location: %s', loc)
                redshift_hook.run(sql_queries.staging_songs_copy.format(self.TABLE, loc, role_arn))
                #redshift_hook.run(sql_queries.staging_songs_copy_key.format(self.TABLE, loc, self.key, self.secret))
                logging.info('Import is done.')
            except Exception as e:
                logging.error('Loading files failed with error: {}'.format(e))

    def _print_context(**context):
        print(kwargs)