import configparser
from datetime import datetime
import os
#import botocore
#import boto3
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')
AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


analytics_tables = ['songs', 'users', 'time', 'artists', 'songplays'] 
staging_tables = ['staging_events', 'staging_songs']
song_columns=['song_id','title','artist_id','year','length']
user_columns=['userid', 'firstname', 'lastname', 'gender', 'level']
time_columns =['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
artist_columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
songplay_export_colums=['songplay_id','start_time', 'userid', 'level', 'song_id', 'artist_id', 'sessionid', 'location', 'useragent', 'year', 'month']
songplay_columns=['songplay_id','start_time', 'userid', 'level', 'song_id', 'artist_id', 'sessionid', 'location', 'useragent']


artist_table_insert = ("""
    select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from staging_songs
    where artist_name is not null
    """)


time_table_insert = ("""
    select ts as start_time,
    extract (hour from start_time) as hour,
    extract (day from start_time) as day,
    extract (week from start_time) as week,
    extract (month from start_time) as month,
    extract (year from start_time) as year,
    extract (dayofweek from start_time) as weekday
    from staging_events
    where ts is not null
    """)


song_table_insert = ("""
    select song_id, title, artist_id, year, duration
    from staging_songs
    where title is not null
    """)


user_table_insert = ("""
    select distinct userid, firstname, lastname, gender, level
    from staging_events
    where (firstname, lastname) is not null
    """)

songplay_table_insert_old = ("""
    select e.ts, e.userid, e.level, s.song_id, s.artist_id, e.sessionid, e.location, e.useragent
    from songs as s
    left outer join staging_events as e
    on e.song like s.title
    where e.page like 'NextSong'
    """)


songplay_table_insert = ("""
    select e.ts, e.userid, e.level, s.song_id, s.artist_id, e.sessionid, e.location, e.useragent
    from staging_events as e
    join staging_songs as s
    on e.song like s.title
    where e.page like 'NextSong' and e.song is not null and s.artist_id is not null
    and e.artist like s.artist_name
    """)


insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]


usr_schema = StructType([ \
    StructField("userid", IntegerType(), False), \
    StructField("firstname", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("level", StringType(), True), \
    ])


song_schema = StructType([ \
    StructField("song_id", StringType(), False), \
    StructField("title", StringType(), True), \
    StructField("artist_id", StringType(), True), \
    StructField("year", IntegerType(), True), \
    StructField("duration", FloatType(), True), \
    ])


artist_schema = StructType([ \
    StructField("artist_id", StringType(), False), \
    StructField("artist_name", StringType(), False), \
    StructField("location", StringType(), True), \
    StructField("latitude", FloatType(), True), \
    StructField("longitude", FloatType(), True), \
    ])


time_schema = StructType([ \
    StructField("start_time", TimestampType(), False), \
    StructField("hour", IntegerType(), False), \
    StructField("day", IntegerType(), False), \
    StructField("week", IntegerType(), False), \
    StructField("month", IntegerType(), False), \
    StructField("year", IntegerType(), False), \
    StructField("dayofweek", IntegerType(), False), \
    ])

songplay_schema = StructType([ \
    StructField("songplay_id", IntegerType(), False), \
    StructField("start_time", TimestampType(), False), \
    StructField("user_id", IntegerType(), False), \
    StructField("level", StringType(), True), \
    StructField("song_id", StringType(), False), \
    StructField("artist_id", StringType(), False), \
    StructField("session_id", StringType(), True), \
    StructField("location", StringType(), True), \
    StructField("user_agent", StringType(), True), \
    ])


def create_spark_session():
    """ This function creates a spark session handler
        Returns: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0,saurfang:spark-sas7bdat:3.0.0-s_2.12') \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", config.get('AWS', 'AWS_ACCESS_KEY_ID'))
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", config.get('AWS', 'AWS_SECRET_ACCESS_KEY'))
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def process_song_data(spark, input_data, output_data):
    """ The Process Song Data function reads all JSON files from the given prefix
        and loads them into the staging songs RDD.
        Then it creates the tables songs and artists and writes them to parquet.
        Return: Nothing
    """

    # read song data file
    print(datetime.now(), ': Reading SONG json files')
    staging_songs = spark.read.json((input_data + 'song_data/*/*/*/*.json'), encoding='UTF-8')
    staging_songs.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs = spark.sql(song_table_insert)
    songs = songs.dropDuplicates(['song_id'])
    songs = spark.createDataFrame(songs.collect(), schema=song_schema)
    songs.createOrReplaceTempView("songs")

    # write songs table to parquet files partitioned by year and artist
    try:
        songs.write.partitionBy(['year', 'artist_id']).mode('overwrite').parquet((output_data + 'songs/songs.parquet'))
        print(datetime.now(), ': Parquet files for table SONGS written')
    except Exception as e:
        print(datetime.now(), ': Writing output files failed: ', e)

    # extract columns to create artists table
    artists = spark.sql(artist_table_insert)
    artists = artists.dropDuplicates(['artist_id'])
    artists = spark.createDataFrame(artists.collect(), schema=artist_schema)
    artists.createOrReplaceTempView("artists")

    # write artists table to parquet files
    try:
        artists.write.parquet((output_data + 'artists/'), mode='overwrite', compression='gzip')
        print(datetime.now(), ': Parquet files for table ARTISTS written')
    except Exception as e:
        print(datetime.now(), ': Writing output files failed: ', e)


def process_log_data(spark, input_data, output_data):
    """ The Process Log Function reads all JSON files under the given prefix
        and transforms the data into analytics tables for users, timestamps
        and songplays (removing duplicates, empty values and calculating new
        values on the fly).
        The resulting tables are stored in <DIRECTORY/TABLE NAME>/<TABLE NAME>.parquet
        Returns: Nothing
    """

    # read log data file
    print(datetime.now(), ': Reading LOG json files')
    staging_events = spark.read.json((input_data + 'log-data/*/*/*.json'))
    staging_events.createOrReplaceTempView("staging_events")

    # filter by actions for song plays

    # extract columns for users table
    users = spark.sql(user_table_insert)
    users = users.filter(col('userid') != '')
    users = users.dropDuplicates(['userid'])
    users = users.withColumn('userid', users.userid.cast(IntegerType()))
    users = spark.createDataFrame(users.collect(), schema=usr_schema)
    users.createOrReplaceTempView("users")

    # write users table to parquet files
    try:
        users.write.parquet((output_data + 'users/'), mode='overwrite', compression='gzip')
        print(datetime.now(), ': Parquet file for table USERS written')
    except Exception as e:
        print(datetime.now(), ': Writing output files failed: ', e)

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df =

    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df

    # read in song data to use for songplays table
    songplays = spark.sql(songplay_table_insert)
    songplays = songplays.withColumn('ts', to_timestamp(songplays.ts/1000))
    songplays = songplays.withColumnRenamed('ts', 'start_time')
    songplays = songplays.withColumn('songplay_id', row_number().over(Window().orderBy("start_time")))
    songplays = songplays.withColumn('userid', songplays.userid.cast(IntegerType()))
    songplays = songplays.select(songplay_columns)
    songplays = spark.createDataFrame(songplays.collect(), schema=songplay_schema)
    songplays.createOrReplaceTempView("songplays")

    # extract columns from joined song and log datasets to create songplays table
    # songplays_table =

    # write songplays table to parquet files partitioned by year and month
    try:
        songplays = songplays.withColumn('year', year(songplays.start_time)).withColumn('month', month(songplays.start_time))
        songplays.write.parquet((output_data + 'songplays/'), mode='overwrite', compression='gzip', partitionBy=['year', 'month'])
        print(datetime.now(), ': Parquet files for table SONGPLAYS written')
    except Exception as e:
        print(datetime.now(), ': Writing output files failed: ', e)
    
    # extract columns to create time table
    time = songplays.select('start_time')
    time = time.withColumn('hour', hour('start_time')).withColumn('hour', hour('start_time')).withColumn('day', dayofmonth('start_time')).withColumn('week', weekofyear('start_time')).withColumn('month', month('start_time')).withColumn('year', year('start_time')).withColumn('dayofweek', dayofweek('start_time'))
    time = spark.createDataFrame(time.collect(), schema=time_schema)
    time.createOrReplaceTempView("time")

    # write time table to parquet files partitioned by year and month
    try:
        time.write.parquet((output_data + 'time/'), mode='overwrite', compression='gzip', partitionBy=['year', 'month'])
        print(datetime.now(), ': Parquet files for table TIME written')
    except Exception as e:
        print(datetime.now(), ': Writing output files failed: ', e)


def write_to_parquet(output_data):
    """ This functions writes all output data to a specified output folder structure.
        A separate directory is used per table and named after the table.
        Returns: Nothing
        """
    try:
        users.write.parquet((output_data + 'users/'), mode='overwrite', compression='gzip')
        time.write.parquet((output_data + 'time/'), mode='overwrite', compression='gzip', partitionBy=['year', 'month'])
        songplays = songplays.withColumn('year', year(songplays.start_time)).withColumn('month', month(songplays.start_time))
        songplays.write.parquet((output_data + 'songplays/'), mode='overwrite', compression='gzip', partitionBy=['year', 'month'])
        print(datetime.now(), ': Parquet files written')
    except Exception as e:
        print(datetime.now(), ': Writing output files failed: ', e)


def create_resource(KEY, SECRET, TYPE):
    """ Create a resource for AWS specified as in TYPE (e.g. S3)
        Returns: AWS Resource"""
    try:
        print(datetime.now(), ': Setting up resource for ', TYPE) 
        aws_cli = boto3.client(TYPE,
                               region_name='us-west-2',
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                               )
    except Exception as e:
        print(datetime.now(), ': FAILED creating resource: ', e)
    return aws_cli


def main():
    spark = create_spark_session()
    #input_data = "/home/workspace/"
    input_data = "s3a://udacity-dend/"
    #output_data = "/home/workspace/"
    output_data = "s3a://udal4p1results/"
    bucket_name = 'udal4p1results'
    location = {'LocationConstraint': 'us-west-2'}
    
    # Check if the specified bucket already exists and create it
    #s3_cli = create_resource(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, 's3')
    #try:
    #    s3_buk = s3_cli.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    #    print(datetime.now(), ': Creating bucket for export data: ', e)
    #except Exception as e:
    #    print(datetime.now(), ': Using existing bucket: ', e)
    
    # Process json files
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    #write_to_parquet(output_data)
    spark.stop()

if __name__ == "__main__":
    main()
