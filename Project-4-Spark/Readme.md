Introduction

This ETL program extracts JSON files containg song data and events stored on Amazon S3, then
transforms the data into analytics tables. The resulting tables are exported using Parquet file
format to an Amazon S3 location.



How to use

You should use this program as follows:
1. Define your output location (an S3 bucket you have write access to) in "etl.py"
2. Enter AWS Key Id and Secret in "dl.cfg"
3. Run the script on an AWS EMR instance
    - copy "etl.py" to the instance
    - run the script using the command "spark-submit"
      Example: "spark-submit --master yarn etl.py"
4. Copy the resulting tables from S3 to your analysis tool and import the data



Data Model
JSON files are loaded into the EMR cluster from S3 in "Schema on Read" mode with NO alterations
of the columns or values. This results in two dataframes, staging_events and staging_songs.
From these two dataframes the analytics tables are created using Spark SQL commands. The select
statements are the ones from the previous project with the exception of the "insert into" 
command which is replaced by a new Spark dataframe.
Columns 
are cleaned from empty entries (like ""), duplicates, etc. Types are casted as appropriate e.g.
from strings to integers.
The songplay_id is a primary key and is based on a simple row number calculation once all other
data is inserted.
After the cleaning the scripts writes the table to disk using parquet format. Partitions for
tables songs, songplays and time are defined within this step.
The resulting parquet files are stored in the S3 store defined in variable "output_data".

Because the songs are exported in partitions by year and artist_id this process creates lots
of files in sizes of few KB (14.549 parquet files result which total in only 15 MB of storage
being consumed.
In standard mode Spark switches to FileOutCommiter Version 1 when creating the parquet files.
Storing the data then takes > 1hr to complete (I never saw a successful completion and aborted
the process).
So the FileOutCommiter is set manually using this config:
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

The m5.xlarge instance in my tests needed about 30 minutes to finish the script.


File description
  I "etl.py"
    The central program to run the complete ETL process, it will do the following:
    1. Read AWS configuration parameters (KEY and SECRET) from "dl.cfg"
        This is for testing purposes only, please replace that section with role-based-access
        in production environments.
    2. Check if the S3 target location is existing (if not, itś trying to create a bucket)
        --> this is turned off in the submitted project
    3. Read all json EVENT and SONG files from "s3a://udacity-dend/log_data" and ".../song_data"
    4. It will load the JSON files into two Spark dataframe and clean the data:
        - missing userids are removed
        - userids are converted from string to integer
        - timestamp "bigints" are casted to data type timestamp
        Blank and empty cells will be imported with a NULL value
        write_to_parquet
    5. The data in the staging tables is then transformed and cleaned before the script loads it
        into the analytics tables
        Transforming means:
         - NULL values of song_id, artist_id and user_id will be removed,
         - Timestamps will be converted into hours, days, weekdays, weeks, months and years