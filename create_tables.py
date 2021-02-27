import configparser
import psycopg2
import boto3
import sys
import getopt
import logging
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
from sql_queries import create_table_queries, drop_table_queries, dist_schema, search_path, create_redshift_tables


"""The scrip "create_tables.py" has the following tasks:
- Read configuration data for Redshift database and S3 storage
- Waits for a running redshift instance belonging to the cluster described in dwh.cfg
- Create a connection to the DB and return a cursor
- Drop existing database content and create new tables (see "sql_queries.py" for details)
"""

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s \t %(message)s ',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
log = logging.getLogger('log')

# Standard Header and Command Line Arguments
schema = 'public'
try:
    argv = sys.argv[1:]
    opts, args = getopt.getopt(argv, 's:')
    log.info(f"Name of the script        : {sys.argv[0]}")
    log.info(f"Arguments of the script   : {sys.argv[1:]}")
    for o, p in opts:
        if o == '-s':
            schema = p
            log.info('Setting schema to: {}'.format(p))
except:
    log.warning('No command line arguments given, using default values')


def create_client(KEY, SECRET, TYPE):
    """ Uses provideds authentication data to create a boto3 client
        of desired type (redshift, ec2, s3).
        Returns: AWS Client Object of specified type
        """
    # Uses key and secret to create a boto3 client of type redshift, ec2, etc.
    # Returns: aws client connection
    try:
        log.info('Setting up Boto3 client for {}'.format(TYPE))
        awsclient = boto3.client(
            TYPE, region_name='us-west-2', aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
            )
    except Exception as e:
        log.error('FAILED creating Boto3 client: ', e)
    return awsclient


def create_resource(KEY, SECRET, TYPE):
    """ Uses provideds authentication data to create a boto3 resource
        of desired type (redshift, ec2, s3).
        Returns: AWS Resource Object of specified type
        """
    try:
        log.info(': Setting up Boto3 resource for {}'.format(TYPE))
        awsclient = boto3.resource(
            TYPE, region_name='us-west-2', aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
            )
    except Exception as e:
        log.error('FAILED creating Boto3 resource: {}', e)
    return awsclient


def drop_all_tables(cur, conn):
    # Connect to DB and drop the tables to start from scratch
    # Returns: Nothing
    for query in drop_table_queries:
        log.info('Running query: {}'.format(query))
        cur.execute(query)
        conn.commit()


def create_all_tables(cur, conn):
    # Connect to DB and define new table setup
    # Returns: Nothing
    for query in create_redshift_tables:
        cur.execute(query)
        conn.commit()
  

def main():
    # Read configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY                    = config.get('AWS', 'KEY')
    SECRET                 = config.get('AWS', 'SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB                 = config.get('DWH', 'DWH_DB')
    DWH_DB_USER            = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD        = config.get('DWH', 'DWH_DB_PASSWORD')
    DWH_PORT               = config.get('DWH', 'DWH_PORT')
    
    # Create REDSHIFT CLIENT and WAIT until instance can be connected ("etl.py" starts the instance)
    redshift_client = create_client(KEY, SECRET, 'redshift')
    redshift_waiter = redshift_client.get_waiter('cluster_available')
    redshift_waiter.wait(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
    log.info('WAITING for Cluster ...')
    
    # When redshift instance is running, connect to its endpoint and reset tables
    myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    try:
        log.info('Cluster is reachable, OK')
        log.info('Trying db connection with...')
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
        log.info('SUCCESS connecting {0} on Host {1} via Port {2}'.format(DWH_DB, DWH_ENDPOINT, DWH_PORT))
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        reachable = 'yes'
    except Exception as e:
        log.error(e)
        reachable = 'no'
    
    if reachable == 'yes':
        try:
            cur.execute('SELECT * FROM staging_songs LIMIT 1;')
            table_exist = cur.fetchone('')
            log.warning('A table staging_songs already exists, there is ALREADY some data')
            reset = input('Would you like to reset the tables anyways (yes/no)? ')
        except:
            table_exist = 'no'
    if (table_exist == 'no' or reset == 'yes'):
        cur.execute(dist_schema.format(schema))
        cur.execute(search_path.format(schema))
        drop_all_tables(cur, conn)
        create_all_tables(cur, conn)
        log.info('Tables initialized')
        conn.close()
    else:
        log.info('Doing nothing')
        conn.close()
    

if __name__ == "__main__":
    main()
