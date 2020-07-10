import configparser


# CONFIG FOR DATA WAREHOUSE
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES IF EXISTS

staging_events_table_drop = "DROP TABLE IF EXISTS event_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATEING TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS event_staging (
    artist VARCHAR,
    auth VARCHAR,
    firstname VARCHAR,
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId BIGINT,
    song VARCHAR,
    status VARCHAR,
    ts VARCHAR,
    userAgent VARCHAR,
    userId BIGINT)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS song_staging (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration NUMERIC,
    year INT)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL DISTKEY SORTKEY,
    user_id BIGINT NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id BIGINT,
    location VARCHAR,
    user_agent VARCHAR)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT NOT NULL SORTKEY PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR(1),
    level VARCHAR)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration NUMERIC)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL SORTKEY PRIMARY KEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT)
""")

# COPYING DATA TO STAGING TABLES

staging_events_copy = ("""
COPY event_staging
FROM {0}
IAM_ROLE {1}
FORMAT AS JSON {2};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY song_staging
FROM {0}
IAM_ROLE {1}
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES TO LOAD DATA FROM STAGING TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + (CAST(est.ts AS BIGINT)/1000) * INTERVAL '1 second' AS start_time,
    est.userId,
    est.level,
    ISNULL(s.song_id, 'NULL'),
    ISNULL(s.artist_id, 'NULL'),
    est.sessionId,
    est.location,
    est.userAgent
    FROM event_staging est
    LEFT JOIN 
            (
            SELECT s.*, a.name as artist_name 
            FROM songs s
            JOIN artists a
            ON s.artist_id = a.artist_id
            ) AS s
        ON est.song = s.title
        AND est.length = s.duration
        AND est.artist = s.artist_name
WHERE est.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
SELECT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM (
    SELECT userId, firstName, lastName, gender, level,
    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY CAST(ts AS BIGINT) DESC) as rows
    FROM event_staging
    WHERE userId IS NOT NULL AND page = 'NextSong'
    ) subq
WHERE
    rows=1
""")

song_table_insert = ("""
INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
SELECT
    song_id,
    title,
    artist_id,
    CASE 
        WHEN year=0 
        THEN NULL 
        ELSE year 
        END,
    duration
FROM
    song_staging
""")

artist_table_insert = ("""
INSERT INTO artists 
    (artist_id, name, location, latitude, longitude)
SELECT
    artist_id,
    artist_name,
    CASE 
        WHEN artist_location='' 
        THEN NULL 
        ELSE artist_location 
        END,
    CASE 
        WHEN artist_latitude=0 
        THEN NULL 
        ELSE artist_latitude 
        END,
    CASE 
        WHEN artist_longitude=0 
        THEN NULL
        ELSE artist_longitude
        END
FROM (
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
    ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY year DESC) AS rows
    FROM song_staging     
    ) subq
WHERE 
    rows=1
""")

#https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html
time_table_insert = ("""
INSERT INTO time 
    (start_time, hour, day, week, month, year, weekday)
SELECT
    ts,
    DATE_PART(hr, ts),
    DATE_PART(d, ts),
    DATE_PART(w, ts),
    DATE_PART(mon, ts),
    DATE_PART(y, ts),
    DATE_PART(weekday, ts)
FROM (
    SELECT DISTINCT TIMESTAMP 'epoch' + (CAST(ts AS BIGINT)/1000)* INTERVAL '1 second' AS ts
    FROM event_staging
    WHERE page = 'NextSong'
) subq
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
