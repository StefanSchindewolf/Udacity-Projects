import configparser
import psycopg2
import botocore
import boto3
import pandas as pd
import json
import sys
import socket
from time import time
from datetime import datetime
from botocore.exceptions import ClientError
from sql_queries import *

'''The scrip 'etl.py' has the following tasks:
- Read configuration data for Redshift database and S3 storage
- IF user chooses 'yes' during execution, a redshift instance is started
- Waits for a running redshift instance of the cluster configured in dwh.cfg
- Opens tcp connection and establishes a db connection
- Checks if tables already exist and if not creates a set of empty tables
- Extract data from S3 json files
- Transforms data into redshift staging tables for events and songs
- Loads data into Sparkify star schema (sql statements --> sql_queries.py)
- Closes db connection and exits
'''


def import_config_file(*new_arn):
    """ Reads configuration file dwh.cfg and defines a set of global variables
        If a new arn is provided which belongs to the DWH_ROLE_NAME the
        function replaces the old value
        Returns: Dictionary of config values"""
    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER
    global DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_ROLE_NAME, S3_ROOT, SONG_DATA
    global LOG_DATA, LOG_JSONPATH, DWH_ROLE_ARN, MY_IP
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    psycon_param_labels = pd.Series(['DWH_ENDPOINT', 'DWH_DB', 'DWH_DB_USER',
                                     'DWH_DB_PASSWORD', 'DWH_PORT'])
    KEY                    = config.get('AWS', 'KEY')
    SECRET                 = config.get('AWS', 'SECRET')
    DWH_CLUSTER_TYPE       = config.get('DWH', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES          = config.get('DWH', 'DWH_NUM_NODES')
    DWH_NODE_TYPE          = config.get('DWH', 'DWH_NODE_TYPE')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB                 = config.get('DWH', 'DWH_DB')
    DWH_DB_USER            = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD        = config.get('DWH', 'DWH_DB_PASSWORD')
    DWH_PORT               = config.get('DWH', 'DWH_PORT')
    DWH_ROLE_NAME          = config.get('DWH', 'DWH_ROLE_NAME')
    BUCKET                 = config.get('S3', 'S3_BUCKET')
    S3_ROOT                = config.get('S3', 'S3_BUCKET')
    LOG_JSONPATH           = config.get('S3', 'LOG_JSONPATH')
    SONG_DATA              = config.get('S3', 'SONG_DATA')
    LOG_DATA               = config.get('S3', 'LOG_DATA')
    DWH_ROLE_ARN           = config.get('IAM_ROLE', 'DWH')
    MY_IP                  = config.get('AWS', 'MY_IP')
    # If function is called with an ARN let´s put it into the config file
    if new_arn in locals():
        if DWH_ROLE_NAME in new_arn:
            config.set('IAM_ROLE', 'DWH', str(new_arn))
            DWH_ROLE_ARN = new_arn
            print(datetime.now(), ': Updated DWH_ROLE_NAME to ', new_arn)
    return_dict = {
        'KEY': KEY, 'SECRET': SECRET, 'DWH_CLUSTER_TYPE': DWH_CLUSTER_TYPE,
        'DWH_NUM_NODES': DWH_NUM_NODES, 'DWH_NODE_TYPE': DWH_NODE_TYPE,
        'DWH_CLUSTER_IDENTIFIER': DWH_CLUSTER_IDENTIFIER, 'DWH_DB': DWH_DB,
        'DWH_DB_USER': DWH_DB_USER, 'DWH_DB_PASSWORD': DWH_DB_PASSWORD,
        'DWH_PORT': DWH_PORT, 'DWH_ROLE_NAME': DWH_ROLE_NAME,
        'S3_ROOT': S3_ROOT, 'SONG_DATA': SONG_DATA, 'LOG_DATA': LOG_DATA,
        'LOG_JSONPATH': LOG_JSONPATH, 'DWH_ROLE_ARN': DWH_ROLE_ARN,
        'MY_IP': MY_IP
        }
    return return_dict


def drop_all_tables(cur, conn):
    """ Drops all tables in the database, see sql_queries.py for details.
        Returns: Nothing
        """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_all_tables(cur, conn):
    """ Creates all tables in the database, see sql_queries.py for details.
        Returns: Nothing
        """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def create_client(KEY, SECRET, TYPE):
    """ Uses provideds authentication data to create a boto3 client
        of desired type (redshift, ec2, s3).
        Returns: AWS Client Object of specified type
        """
    # Uses key and secret to create a boto3 client of type redshift, ec2, etc.
    # Returns: aws client connection
    try:
        print(datetime.now(), ': Setting up client for ', TYPE)
        awsclient = boto3.client(
            TYPE, region_name='us-west-2', aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
            )
        print(datetime.now(), ': Creating client of type', type(awsclient))
    except Exception as e:
        print(datetime.now(), ': FAILED creating client: ', e)
    return awsclient


def create_resource(KEY, SECRET, TYPE):
    """ Uses provideds authentication data to create a boto3 resource
        of desired type (redshift, ec2, s3).
        Returns: AWS Resource Object of specified type
        """
    try:
        print(datetime.now(), ': Setting up resource for ', TYPE)
        awsclient = boto3.resource(
            TYPE, region_name='us-west-2', aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
            )
    except Exception as e:
        print(datetime.now(), ': FAILED creating resource: ', e)
    return awsclient


def search_bucket(s3handler, prefix, amount=10):
    """ Creates a file list from S3 storage containing the provided prefix
        of its caller (for songs or logs).
        The list is limited to 10 (default) or the integer value provided
        by the user. The files are checked for keyword "json" in filename
        (other file suffixes are ignored).
        Returns: list of JSON files
        """
    amount = amount + 1
    filelist = list()
    if 'song' in prefix:
        searchpath = 'song-data'
    if 'log' in prefix:
        searchpath = 'log_data'
    try:
        results = s3handler.objects.filter(Prefix=searchpath).limit(amount)
    except botocore.exceptions.ResourceError as error:
        print(datetime.now(), ': FAILED to filter objects', error)
    for file in results:
        if 'json' in file.key:
            filelist.append('s3://udacity-dend/%s' % file.key)
    print(
        datetime.now(), ': Finished checking, found ', len(filelist), ' files'
        )
    return filelist


def load_staging_tables(cur, conn, arn, logs, songs):
    """ Takes a list of json files provided in filepath and uses the Redshift Copy command
        to insert the data into staging_tables. Only required if the users wants to process
        a specific number of files instead of all files.
        Returns: Nothing
        """
    # Check if entries in staging tables already exists and if not then create all new tables
    try:
        cur.execute('select count (*) from staging_events')
        entries = cur.fetchone()[0]
        cur.execute('select count (*) from staging_songs')
        entries = entries + cur.fetchone()[0]
    except:
        entries = 0
    if entries > 0:
        print(datetime.now(), ': Found {0} entries in database, database not empty'.format(entries))
        print(datetime.now(), ': Please run script "create_tables.py" to initiate staging tables')
    else:
        t0 = time()
        print(datetime.now(), ': Resetting tables')
        cur.execute(dist_schema)
        cur.execute(search_path)
        drop_all_tables(cur, conn)
        create_all_tables(cur, conn)
        print(datetime.now(), ': Starting copy of songs using prefix ', songs)
        cur.execute(staging_songs_copy.format(songs, arn))
        t1 = time()
        runtime = t1 - t0
        cur.execute('insert into dashboard (step, runtime) values (\'Loading songs from S3\', {})'.format(runtime))
        print(datetime.now(), ': Starting copy of logfiles using prefix ', logs)
        cur.execute(staging_events_copy.format(logs, arn))
        print(datetime.now(), ': Done')
        t2 = time()
        runtime = t2 - t1
        cur.execute('insert into dashboard (step, runtime) values (\'Loading events from S3\', {})'.format(runtime))
        conn.commit()
        print(datetime.now(), ': Running vacuum and analyze')
        cur.execute('vacuum')
        cur.execute('analyze')
        t3 = time()
        runtime = t3 - t2
        cur.execute('insert into dashboard (step, runtime) values (\'Vacuum staging\', {})'.format(runtime))
        conn.commit()
    print(datetime.now(), ': Loading done')


def fill_analytics(cur, conn):
    """ Reads staging tables for songs and events and copies relevant fields
        into analytics tables.
        Returns: Nothing
        """
    # First check if data is already there, if so then remove it
    print(datetime.now(), ': Checking if analytics tables contain data')
    cur.execute('select count (*) from users')
    entries = cur.fetchone()[0]
    if entries > 0:
        print(datetime.now(), ': Tables contain data, dropping them now')
        for dropstm in reset_analytics_tables:
            cur.execute(dropstm)
            # Note: create statements are included in the reset_analytics_tables list
    print(datetime.now(), ': Filling analytics tables')
    t0 = time()
    for query in insert_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(datetime.now(), ': Failed to execute query ', query, ' raising: ', e)
    conn.commit()
    t1 = time()
    runtime = t1 - t0
    cur.execute('insert into dashboard (step, runtime) values (\'Insert from staging\', {})'.format(runtime))
    # Remove duplicates
    candidates = {'songs': 'song_id', 'users': 'user_id', 'artists': 'artist_id'}
    for table, key in candidates.items():
        cur.execute('select count(*) from {}'.format(table))
        before = cur.fetchone()
        for query in remove_duplicates:
            cur.execute(query.format(table, key))
            conn.commit()
        cur.execute('select count(*) from {}'.format(table))
        after = cur.fetchone()
        print(datetime.now(), ': Removed ', (int(before[0]) - int(after[0])), ' duplicate entries from table ', table)
    t2 = time()
    runtime = t2 - t1
    cur.execute('insert into dashboard (step, runtime) values (\'Remove duplicates\', {})'.format(runtime))
    # Cleanup database
    print(datetime.now(), ': Running vacuum and analyze')
    cur.execute('vacuum')
    cur.execute('analyze')
    t3 = time()
    runtime = t3 - t2
    cur.execute('insert into dashboard (step, runtime) values (\'Vacuum analytics\', {})'.format(runtime))
    conn.commit()


def create_db(handler, roleArn):
    """ Read provided arguments and starts a Amazon Redshift instance.
        If uncommented in the "except" section below the user can decide himself if
        he wants to start an instance.
        Returns: Nothing
        """
    try:
        response_alive = handler.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        print(datetime.now(), ': Redshift Cluster already exists, no instance will be started')
        default_sec_group = open_tcp(VPC_ID, MY_IP)
        start = 'no'
    except:
        print(datetime.now(), ': Redshift Cluster not started, initiating cluster start')
        start = 'yes'

    if start == 'yes':
        try:
            response = handler.create_cluster(
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                IamRoles=[roleArn]
            )
            print(datetime.now(), ': Redshift instance now in status: ', str(response.get('ClusterStatus')))
        except Exception as e:
            print(datetime.now(), ': FAILED creating cluster: ', e)


def setup_iam_role(handler):
    '''Using the provided boto3 client the function creates and iam role
    Attaches the S3 read only policy
    Returns: iam role arn'''
    try:
        dwhRole = handler.create_role(
            Path='/',
            RoleName=DWH_ROLE_NAME,
            Description='Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [
                    {
                        'Action': 'sts:AssumeRole', 'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                    'Version': '2012-10-17'}
                )
                )
        print(datetime.now(), ': Creating IAM role ', DWH_ROLE_NAME)
    except Exception as e:
        print(datetime.now(), ': FAILED creating IAM role ', e)
    try:
        handler.attach_role_policy(
            RoleName=DWH_ROLE_NAME, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
            )['ResponseMetadata']['HTTPStatusCode']
        roleArn = handler.get_role(RoleName=DWH_ROLE_NAME)['Role']['Arn']
        print(datetime.now(), ': Attaching IAM policy to role (or it has been done already) ', DWH_ROLE_NAME)
        print(datetime.now(), ': Received ARN ', roleArn)
        return roleArn
    except Exception as e:
        print(datetime.now(), ': FAILED attaching IAM policy ', e)


def remove_iam_role(handler):
    """ Revokes policy for S3 access which was assigned in function 'setup_iam_role'
        Returns: Nothing"""
    handler.detach_role_policy(RoleName=DWH_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    print(datetime.now(), ': Revoked policy: AmazonS3ReadOnlyAccess')
    handler.delete_role(RoleName=DWH_ROLE_NAME)
    print(datetime.now(), ': Role ', DWH_ROLE_NAME, ' deleted')


def open_tcp(vpc_id, my_ip):
    """ Creates a VPC security group and adds a rule to allow db connections from outside
        Returns: Security Group
        """
    try:
        ec2_vpc = create_resource(KEY, SECRET, 'ec2')
        vpc = ec2_vpc.Vpc(vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp=my_ip,
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
        print(datetime.now(), ': Succesfully authorized IP ', my_ip, ' to security group', defaultSg)
        return defaultSg
    except Exception as e:
        print(datetime.now(), ': FAILED creating security group ', e)


def close_tcp(vpc_id, my_ip):
    """ Revokes access to VPC for external IPs provided above
        Returns: Security Group
        """
    try:
        ec2_vpc = create_resource(KEY, SECRET, 'ec2')
        vpc = ec2_vpc.Vpc(vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.revoke_ingress(
            GroupName='default',
            CidrIp=my_ip,
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
        print(datetime.now(), ': Succesfully revoked access for IP ', my_ip, ' to security group', defaultSg)
        return defaultSg
    except Exception as e:
        print(datetime.now(), ': FAILED creating security group ', e)


def print_db_info(cur):
    """ Prints the size of database tables as information
        Returns: Nothing
        """
    cur.execute('select count (*) from staging_events')
    print(datetime.now(), ': Inserted ', int(cur.fetchone()[0]), ' events in staging area')
    cur.execute('select count (*) from staging_songs')
    print(datetime.now(), ': Inserted ', int(cur.fetchone()[0]), ' songs in staging area')
    cur.execute('select count (user_id) from users')
    print(datetime.now(), ': Inserted ', int(cur.fetchone()[0]), ' users')
    cur.execute('select count (start_time) from time')
    print(datetime.now(), ': Inserted ', int(cur.fetchone()[0]), ' timestamps')
    cur.execute('select count (artist_id) from artists')
    print(datetime.now(), ': Inserted ', int(cur.fetchone()[0]), ' artists')
    cur.execute('select count (song_id) from songs')
    print(datetime.now(), ': Inserted ', int(cur.fetchone()[0]), ' songs')
    cur.execute('select count (songplay_id) from songplays')
    print(datetime.now(), ': Inserted ', int(cur.fetchone()[0]), ' songplay records')


def prettyRedshiftProps(props):
    """ Takes a given set of Redshift instance properties
        Returns: DataFrame with properties"""
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername', 'DBName', 'Endpoint', 'NumberOfNodes', 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=['Key', 'Value'])


def main(args=None):
    """ The main function executes all central program steps:
            - Reading the dwh.cfg
            - Using the configuration to setup an IAM role
            - Creating a Redshift database instance
            - Using the Redshift instance to read JSON files from S3
            - Inserting JSON content into staging tables
            - Filling analytics tables from staging tables (including removing duplicates
            - Deleting the instance and removing the IAM role)"""
    print(datetime.now(), ': STARTING SPARKIFYDB DATA LOAD')

    # Import configuration data
    print(datetime.now(), ': Reading config file')
    config_dict = import_config_file()

    # Create Redshift and ec2 clients
    redshift_client = create_client(KEY, SECRET, 'redshift')
    ec2_client = create_client(KEY, SECRET, 'ec2')
    iam_client = create_client(KEY, SECRET, 'iam')
    s3 = create_resource(KEY, SECRET, 's3')
    print(datetime.now(), ': Creating AWS clients and resources done')

    # Setup IAM Role and write to config if not exists
    iam_role_arn = setup_iam_role(iam_client)
    import_config_file(iam_role_arn)

    # Start instance if required (function will check if cluster is already up)
    create_db(redshift_client, iam_role_arn)

    # Create a Redshift waiter to wait for instance being started
    redshift_waiter = redshift_client.get_waiter('cluster_available')
    print(datetime.now(), ': Waiting for instance...')
    redshift_waiter.wait(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)

    # Get endpoint and iam role arn, add to config fields
    myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    VPC_ID = myClusterProps['VpcId']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    # Open TCP Port
    default_sec_group = open_tcp(VPC_ID, MY_IP)

    # Connect to database and get a cursor
    try:
        conn = psycopg2.connect('host={0} dbname={1} user={2} password={3} port={4}'.format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        print(datetime.now(), ': SUCCESS connecting {0} on Host {1} via Port {2}'.format(DWH_DB, DWH_ENDPOINT, DWH_PORT))
        retries = 0
    except Exception as e:
            print(datetime.now(), ': FAILED connecting {0} on Host {1} via Port {2}'.format(DWH_DB, DWH_ENDPOINT, DWH_PORT))
            print(e)

    # Use Redshift copy command to load files into staging tables
    if 'cur' in locals():
        try:
            print(datetime.now(), ': Trying to import files using credentials as: ', DWH_ROLE_ARN)
            # Fill staging tables
            load_staging_tables(cur, conn, DWH_ROLE_ARN, LOG_DATA, SONG_DATA)
        except Exception as e:
            print(datetime.now(), ': Failed Loading JSON files ', e)

    # Fill analytics tables
    if 'cur' in locals():
        try:
            fill_analytics(cur, conn)
            # Show number of entries in all tables
            print_db_info(cur)
        except Exception as e:
            print(datetime.now(), ': Failed loading data into tables ', e)

    print(datetime.now(), ': DATA LOAD FINISHED')

    # Clean up everything
    if 'conn' in locals():
        conn.close()
    redshift_client.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    revoked_tcp = close_tcp(VPC_ID, MY_IP)
    remove_iam_role(iam_client)

if __name__ == '__main__':
    main()
