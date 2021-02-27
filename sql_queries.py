import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
events_table_drop = "DROP TABLE IF EXISTS events;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"
staging_events_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_drop = "DROP TABLE IF EXISTS staging_songs"
dashboard_drop = "DROP TABLE IF EXISTS dashboard"

# Lists of tables and fields
analytics_tables = ['songs', 'users', 'time', 'songplays', 'artists'] 
staging_tables = ['staging_events', 'staging_songs']
            
temp_stage_tables = {'songs': 'st_songs', 'users': 'st_users', 'time': 'st_time', 'songplays': 'st_songplays', 
                     'artists': 'st_artists'}
song_stage_columns=['st_song_id', 'num_songs', 'artist_id', 'artist_latitude', 'artist_longitude', 
                        'artist_location', 'artist_name', 'song_id', 'title', 'length', 'year']
event_stage_columns=['st_event_id', 'artist', 'auth', 'firstname', 'gender', 'iteminsession', 
                         'lastname', 'length', 'level', 'location', 'method', 'page', 'registration', 
                         'sessionid', 'song', 'status', 'ts', 'useragent', 'userid']
song_columns=['song_id','title','artist_id','year','length']
user_columns=['userid', 'firstname', 'lastname', 'gender', 'level']
time_columns =['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
artist_columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
songplay_columns=['ts', 'userid', 'level', 'song_id', 'artist_id', 'sessionid', 'location', 'useragent']

# CREATE ANALYTICS TABLES
dist_schema = ("""CREATE SCHEMA IF NOT EXISTS {};""")
search_path = ("""SET search_path TO {};""")
songplay_table_create = (
    """CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1), start_time TIMESTAMP NOT NULL, user_id VARCHAR NOT NULL, level VARCHAR,
    song_id VARCHAR NOT NULL distkey, artist_id VARCHAR NOT NULL, session_id VARCHAR, location VARCHAR,
    user_agent VARCHAR, PRIMARY KEY (songplay_id)
    );"""
)
user_table_create = (
    """CREATE TABLE IF NOT EXISTS users (
    user_id INT sortkey, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR, PRIMARY KEY (user_id)
    );"""
)
song_table_create = (
    """CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR  sortkey, title VARCHAR NOT NULL, artist_id VARCHAR, year INT, duration FLOAT, PRIMARY KEY (song_id)
    );"""
)
artist_table_create = (
    """CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR sortkey, artist_name VARCHAR NOT NULL, location VARCHAR, latitude FLOAT,
    longitude FLOAT, PRIMARY KEY (artist_id)
    );"""
)
time_table_create = (
    """CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP sortkey, hour INT, day INT, week INT, month INT, year INT,
    weekday INT, PRIMARY KEY (start_time)
    );"""
)

# CREATE STAGING TABLES
staging_events_table_create= (
    """CREATE TABLE staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
        );"""
)
staging_songs_table_create = (
    """CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
        );"""
)

# Create dashboard table
dashboard_create = (
    """create table dashboard (
    step VARCHAR, runtime FLOAT)""")

# STAGING TABLES INSERT
staging_songs_copy = (
    """copy staging_songs
    from '{0}'
    iam_role '{1}'
    region 'us-west-2'
    format as json 'auto ignorecase'
    compupdate off
    blanksasnull
    emptyasnull
    ;"""
)
staging_events_copy = (
    """
    copy staging_events
    from '{0}'
    iam_role '{1}'
    region 'us-west-2'
    format as json 's3://udacity-dend/log_json_path.json'
    timeformat as 'epochmillisecs'
    compupdate off
    blanksasnull
    emptyasnull
    ;"""
)

# ANALYTICS TABLES INSERTS
songplay_table_insert = (
    """insert into songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select e.ts, e.userid, e.level, s.song_id, s.artist_id, e.sessionid, e.location, e.useragent
    from staging_events as e
    join staging_songs as s
    on e.song like s.title
    where e.page like 'NextSong' and e.song is not null and s.artist_id is not null
    and e.artist like s.artist_name
    ;"""
)
user_table_insert = (
    """insert into users
    select e.userid::integer, e.firstname, e.lastname, e.gender, e.level
    from staging_events as e
    where (firstname, lastname) is not null
    and e.page like 'NextSong'
                        ;""")
song_table_insert = (
    """insert into songs
    select song_id, title, artist_id, year, duration
    from staging_songs
    where (song_id, title) is not null
    ;"""
)
artist_table_insert = (
    """insert into artists
    select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from staging_songs as s
    join staging_events as e
    on s.title like e.song
    where (s.artist_id, s.artist_name) is not null
    and e.page like 'NextSong'
    ;"""
)
time_table_insert = (
    """insert into time
    select distinct e.ts as start_time,
    extract (hour from start_time) as hour,
    extract (day from start_time) as day,
    extract (week from start_time) as week,
    extract (month from start_time) as month,
    extract (year from start_time) as year,
    extract (dow from start_time) as weekday
    from staging_events as e
    where e.ts is not null
    and e.page like 'NextSong'
    ;"""
)

# Duplicate remove action
table_new_drop = ("""drop table if exists new_{0}""")
table_temp_drop = ("""drop table if exists temp_{0}""")
table_temp_create = (
    """create temp table duplicate_{0} as
    select {1}
    from {0}
    group by {1}
    having count(*) > 1
    ;"""
)
table_new_create = ("""create temp table new_{0} (like {0});""")
table_new_insert = (
    """insert into new_{0}
    select distinct *
    from {0}
    where {1} is not null and {1} in (
        SELECT {1}
        FROM duplicate_{0}
        )
        ;"""
)
table_delete = (
    """delete from {0}
    where {1} is not null and {1} in (
        select {1}
        from duplicate_{0}
        )
        ;"""
)
table_unique_insert = (
    """insert into {0} select * from new_{0};
    """)

create_redshift_tables = [(
    """CREATE TABLE public.artists ( artistid varchar(256) NOT NULL, name varchar(256), location varchar(256), lattitude numeric(18,0), longitude numeric(18,0)); """),
    ("""CREATE TABLE public.songplays (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        level varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );
    CREATE TABLE public.songs (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    CREATE TABLE public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    CREATE TABLE public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );
    CREATE TABLE public."time" (
        start_time timestamp NOT NULL,
        "hour" int4,
        "day" int4,
        week int4,
        "month" varchar(256),
        "year" int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    CREATE TABLE public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
""")]

# QUERY LISTS
create_table_queries = [dashboard_create, staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [dashboard_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, staging_events_drop, staging_songs_drop]
reset_analytics_tables = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
remove_duplicates = [table_new_drop, table_temp_drop, table_temp_create, table_new_create, table_new_insert, table_delete, table_unique_insert]
