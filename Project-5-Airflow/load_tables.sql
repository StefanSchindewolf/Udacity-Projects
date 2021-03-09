# INSERT RECORDS STATEMENTS

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
