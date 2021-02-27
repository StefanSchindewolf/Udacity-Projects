# Introduction
This ETL program extracts two sets of JSON files stored on Amazon S3, one is transforms the data and loads it
into a set of analytics tables


# How to use
You should use this program as follows:
1. Run the script "etl.py" always first
   It will start an Amazon Redshift instance and fill all staging and analytics tables
2. Run the script "create_tables.py" to reset all tables in Redshift, after its finished the tables
   will be empty
3. Run "etl.py" after resetting the tables and it will skip the Redshift cluster deployment and just
   load the table contents again from S3 and fill analytics tables from those staging tables
4. Run "etl.py" while the staging tables contain at least 1 row will result in the script just
   filling the analytics tables, no data from S3 will be loaded
5. Run "Dashboard.ipynb" (a Jupyter notebook) and it will show you an excerpt from the database tables
   and some diagrams with basic statistics
  
  
# Data Model
JSON files are loaded into staging tables with blank or empty values as Null. Timestamps for events
are imported and converted to Redshift timestamps.
The analytics tables are created from the staging data by removing data not matching the "Not Null"
constraints. Timestamps are converted into hours, days, weeks, weekdays, months and years using
Redshift functions.
A "Duplicate Removal" action is introduced that removes duplicate entries from all analytics tables
by using a temporary table of distinct rows and writing them back to the original table.
The fact table songplays is created by matching the title and song from both staging tables with
the constraint that the event is a NextSong action and the artist name is also matching. However
this may result in some duplicates due differences in song duration in both datasets. This effect
is measured in the data quality dashboard.


# Description of project files
1. **etl.py**
The central program to run the complete ETL process, it will do the following:
    1. Read all configuration parameters from "dwh.cfg"
        The configuration variables from this file are named in upper case, e.g. "DWH_DB"
        and declared as "global" so that we do not have to include them in every function call
    1. Check if an Redshift instance is running and if not
        it will create a role and attach an IAM policy to it and start the instance
    1. Using the IAM role it will download all JSON file found in the given S3 bucket
    1. It will load the JSON files using the Redshift "copy" command
        For events the JSON Path File will be used to map the event description to the staging table
        Songs will be imported 
        Blank and empty cells will be imported with a NULL value
    1. The data in the staging tables is then transformed and cleaned before the script loads it
        into the analytics tables
        Transforming means:
         - NULL values of song_id, artist_id and user_id will be removed,
         - Timestamps will be converted into hours, days, weekdays, weeks, months and years
         - Duplicates will be removed
    1. Delete the Redshift instance and remove the policy and role
        The instance is stored as a snapshot
1. **create_tables.py**
A script to reset the tables, it works as follows:
    1. Read all configuration parameters from "dwh.cfg"
    2. Identify the Redshift cluster node name
    3. Connect to the node and run SQL commands to drop all tables and recreate them
1. **dwh.cfg**
A configuration file which contains all fixed parameters for the script
    The file is accessed using Python's ConfigParser module
 IV "sql_queries.py"
    All SQL queries required for the ETL process are stored as string variables
    Three main categories of queries exist: drop tables, create tables and insert data
    into those tables (including the copy command)
    At the bottom of the file the SQL commands are assigned to a set of execution lists
    Those lists contain the SQL queries in a logical order for execution, e.g. to insert
    a songplay record only after the records for users, timestamps, songs and artists
    were inserted
  V "Dashboard.ipynb"
    This Jupyter Notebook will show some basic data quality checks:
     1. Each table has entries
        Bar graph with number of entries per table
     2. Check for duplicate songs from the same artist
     3. Check for missing artist locations
     4. Check for NextSong actions that cannot be matched to songs
     5. Check for runtimes of each ETL step
     6. Check the Top 10 songs in table songplays
