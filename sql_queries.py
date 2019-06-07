import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS songs;"
song_table_drop = "DROP TABLE IF EXISTS artists;"
artist_table_drop = "DROP TABLE IF EXISTS users;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= """
    CREATE TABLE IF NOT EXISTS staging_events
    (artist TEXT, 
     auth TEXT,
     first_name TEXT,
     gender VARCHAR(6),
     item_in_session SMALLINT,
     last_name TEXT, 
     length NUMERIC, 
     level VARCHAR(8),
     location TEXT,
     method VARCHAR(6),
     page TEXT,
     registration NUMERIC,
     session_id INTEGER,
     song TEXT,
     status INTEGER,
     start_time_ms BIGINT,
     user_agent TEXT,
     user_id INTEGER);
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs
    (artist_id TEXT NOT NULL,
     artist_latitude NUMERIC, 
     artist_longitude NUMERIC, 
     artist_location TEXT, 
     artist_name TEXT, 
     song_id TEXT NOT NULL,
     title TEXT NOT NULL,
     duration NUMERIC,
     year INTEGER);
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays
    (songplay_id INT IDENTITY(1, 1) PRIMARY KEY, 
     start_time_ms BIGINT REFERENCES time(start_time_ms), 
     user_id INTEGER REFERENCES users(user_id), 
     level TEXT, 
     song_id TEXT REFERENCES songs(song_id), 
     artist_id TEXT REFERENCES artists(artist_id), 
     session_id TEXT, 
     location TEXT, 
     user_agent TEXT);
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time
    (start_time_ms BIGINT PRIMARY KEY,
     start_time TIMESTAMP,
     hour INTEGER, 
     day INTEGER, 
     week INTEGER, 
     month INTEGER, 
     year INTEGER, 
     weekday INTEGER);
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users
    (user_id INTEGER PRIMARY KEY, 
     first_name TEXT, 
     last_name TEXT, 
     gender CHAR(1), 
     level TEXT);
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs
    (song_id TEXT PRIMARY KEY, 
     title TEXT NOT NULL, 
     artist_id TEXT, 
     year INTEGER, 
     duration NUMERIC);
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists
    (artist_id TEXT PRIMARY KEY, 
     name TEXT NOT NULL, 
     location TEXT, 
     lattitude NUMERIC, 
     longitude NUMERIC);
"""

# COPY TO STAGING TABLES

staging_events_copy = """
    COPY staging_events 
    FROM {} 
    credentials {} 
    json {};
""".format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = """
    COPY staging_songs
    from {}
    credentials {}
    json 'auto';
""".format(SONG_DATA, ARN)

# INSERT INTO FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (start_time_ms, user_id, level, song_id, 
                           artist_id, session_id, location, 
                           user_agent)
    SELECT start_time_ms AS start_time_ms,
           user_id       AS user_id,
           level         AS level,
           song_id       AS song_id,
           artist_id     AS artist_id,
           session_id    AS session_id,
           location      AS location,
           user_agent    AS user_agent
    FROM staging_events stge
    LEFT JOIN staging_songs stgs
    ON stge.song=stgs.title AND stge.artist=stgs.artist_name
    WHERE page='NextSong';
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, level)
    SELECT DISTINCT user_id    AS user_id,
           first_name AS first_name, 
           last_name  AS last_name, 
           level      AS level
    FROM staging_events
    WHERE user_id IS NOT NULL;
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id   AS song_id,
           title     AS title,
           artist_id AS artist_id,
           year      AS year,
           duration  AS duration
    FROM staging_songs;
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id AS artist_id,
           artist_name AS name,
           artist_location AS location,
           artist_latitude AS latitude,
           artist_longitude AS longitude
    FROM staging_songs;
"""

time_table_insert = """
    INSERT INTO time (start_time_ms, start_time, hour, day, week, 
                      month, year, weekday)
    SELECT DISTINCT start_time_ms AS start_time_ms, 
           TIMESTAMP 'epoch' + start_time_ms/1000*interval '1 second' AS start_time,
           EXTRACT(hour    FROM TIMESTAMP 'epoch' + start_time_ms/1000*interval '1 second') AS hour,
           EXTRACT(day     FROM TIMESTAMP 'epoch' + start_time_ms/1000*interval '1 second') AS day,
           EXTRACT(week    FROM TIMESTAMP 'epoch' + start_time_ms/1000*interval '1 second') AS week,
           EXTRACT(month   FROM TIMESTAMP 'epoch' + start_time_ms/1000*interval '1 second') AS month,
           EXTRACT(year    FROM TIMESTAMP 'epoch' + start_time_ms/1000*interval '1 second') AS year,
           EXTRACT(weekday FROM TIMESTAMP 'epoch' + start_time_ms/1000*interval '1 second') AS weekday
    FROM staging_events;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]