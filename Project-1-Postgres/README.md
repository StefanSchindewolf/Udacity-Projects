# PROJECT SUMMARY
Sparkify has published a new music player app which customers now regularly use.
This set of files contains ETL code and a data sample for testing purposes.
The scripts "create_tables.py" and "etl.py" are used for setting up the database and create (exemplatory) content.
The ETL code in "etl.py" extracts data files and log files, transforms them into what songs users were playing (plus some metadata) and timestamps of those actions.
This data is then loaded into the database "sparkifydb" for further analysis.

# INSTRUCTIONS
- Download and extract the project files to your home directory or workspace</IL>
- You should have a directory tree "data" with lots of json files in that tree and the files "create_tables.py" and "etl.py"</IL>
- The jupyter files are for development and testing purposes only and can be ignored for the moment</IL>
- Open a terminal window and change to the project directory</IL>
- First run "python3 create_tables.py" to create the database and tables</IL>
- Secondly run "python3 etl.py" to extract the business data from the json files</IL>
- Open a jupyter notebook and run "test.ipynb" to check the results</IL>

# HOW IT WORKS
## Data Source
- Files in directory data are stored in json format
- Log data and song data files are stored in different directories
- Log data files in subfolders per year and month
- Song data files in alphabetical subfolders
- The complete tree "data" is searched recursively for files ending with ".json"
- Other file extensions are ignored

## Processing
- slq_queries.py: file contains all core SQL statements and will be imported to both "create_tables.py" and "etl.py"
- create_tables.py: prepares our "landing ground", a database with all tables for our resulting data
- etl.py:
    - Is setting up the star schema for analysis
    - Fills tables "songs" and "artists" from song data files
    - Fills tables "users" and "time" from log data files
    - Searches tables "songs" and "artists" for song data matching the song a user played at a specific time and date
    - Adds user data to the dataset and stores each case as a song play action in table "songplays"

## ETL Results and star schema
- A new database "sparkifydb" is created with tables songplays users, artists, songs and time
- Table "songplays" creates the business data to be analyzed
- Table "time" can be used to analyze songplays over the course of time, e.g. months, weekdays and so on
- Further details from "songs" and "artists" can be applied to analysis, e.g. locations of an artist or publishing year of a song
- Users are stored in table "users" and can be selected to analyze gender or subscription status of individuals
- You may add data from these table by joining them to your songplay actions using fields
    - user_id for user data (e.g. gender)
    - artist_id for artist data (e.g. lcoation)
    - start_time for time series data (days, hours, etc.)
    - song_id for song data (e.g. the year a song was published)
