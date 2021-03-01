import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Takes the json file provided in filepath and reads the file
    Then song data and artist data are selected and inserted into the corresponding tables"""
    # Open song file using the path variable "filepath" and create a dataframe from it
    df = pd.read_json(filepath, lines=True)
    
    # Insert songs record
    # Select song data columns from df and store in a new dataframe
    songs_table_columns = df[["song_id","title","artist_id","year","duration"]]
    # Select only the values from df and store in a list
    song_data = songs_table_columns.values[0].tolist()
    # Write to songs table
    cur.execute(song_table_insert, song_data)
    
    # Insert artist record
    # Select artist columns and store in new dataframe
    artist_table_columns = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    # Create a list
    artist_data = artist_table_columns.values[0].tolist()
    # Write to artists table
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''Process the log file provided in filepath and store all song play actions of users:
    1. Search file for NextSong actions of users
    2. Collect timestamps of these actions
    3. Select user data and load them into table users
    4. Search for song and artists ids in tables songs and artists
    5. Add recorded song play actions from users to table songplays'''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query('page == "NextSong"')

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms').sort_values()
    
    # insert time data records, with duplicates removed
    time_data = (t.dt.values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    # First a dictionary with all key-value pairs
    dict_ts = dict(zip(column_labels, time_data))
    # Then a dataframe from the dictionary
    time_df = pd.DataFrame.from_dict(dict_ts)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    pd.options.mode.chained_assignment = None
    user_df['userId'] = pd.to_numeric(user_df['userId'].values.tolist(), errors='ignore')
    # Clean the data from duplicated and NaN entries, sort the df
    user_df.dropna(axis=0, inplace=True)
    user_df = user_df.drop_duplicates()
    user_df = user_df.sort_values(by='userId')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # First get song title, artist name and song length and run SELECT for 
        selectors = (row.song, row.artist, row.length)
        cur.execute(song_select, selectors)
        results = cur.fetchone()
        
        # If the SELECT yields any results, complete the dataset and insert it into the table
        if results:
            # Store song and artist ids from the SELECT separately
            songid, artistid = results
            # insert songplay record
            # Convert the timestamps to datetime
            row.ts = pd.to_datetime(row.ts, unit='ms')
            # Copy all other required fields including songid, artistid and timestamp
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            # Write to table
            cur.execute(songplay_table_insert, songplay_data)
        else:
            songid, artistid = None, None


def process_data(cur, conn, filepath, func):
    """Recursively search given filepath and process all json files.
    Calls for each file the function "func" (which is either directing from here to "process_song_file" or "process_log_file
    Nothing returned from here, but the changes from the sub-functions are committed"""
    # et all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()