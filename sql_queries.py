import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events cascade"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs cascade"
songplay_table_drop = "DROP TABLE IF EXISTS songplays cascade"
user_table_drop = "DROP TABLE IF EXISTS users cascade"
song_table_drop = "DROP TABLE IF EXISTS songs cascade"
artist_table_drop = "DROP TABLE IF EXISTS artists cascade"
time_table_drop = "DROP TABLE IF EXISTS time cascade"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
    staging_events_id BIGINT IDENTITY(1,1),
    artist VARCHAR(255),
    auth VARCHAR(50),
    first_name VARCHAR(50),
    gender VARCHAR(10),
    item_in_session INT,
    last_name VARCHAR(50),
    length FLOAT,
    level VARCHAR(20),
    location VARCHAR(255),
    method VARCHAR(20),
    pave VARCHAR(30),
    registration INT8,
    session_id INT,
    song VARCHAR(255),
    status INT,
    ts INT8,
    user_agent VARCHAR(255),
    user_id INT,
    PRIMARY KEY(staging_events_id)
    );
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
    staging_songs_id BIGINT IDENTITY(1,1),
    num_songs INT,
    artist_id VARCHAR(50),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(50),
    title VARCHAR(255),
    duration FLOAT,
    year INT,
    PRIMARY KEY(staging_songs_id)
    );
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGINT IDENTITY(1,1), 
    start_time TIMESTAMP, 
    user_id INT NOT NULL, 
    level VARCHAR, 
    song_id VARCHAR(100),
    artist_id VARCHAR(100),
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users(
    user_id INT NOT NULL, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR,
    PRIMARY KEY(user_id)
    );
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(100) NOT NULL, 
    title VARCHAR(255), 
    artist_id VARCHAR(100) NOT NULL, 
    year INT, 
    duration int, 
    PRIMARY KEY(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(100), 
    name VARCHAR(255), 
    location VARCHAR(255), 
    latitude FLOAT, 
    longitude FLOAT,
    PRIMARY KEY(artist_id)
    );
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP, 
    hour INT, 
    day INT, 
    week INT,
    month INT, 
    year INT, 
    weekday INT,
    PRIMARY KEY(start_time)
    );
"""

# STAGING TABLES

staging_events_copy = """
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""".format(
    config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"]
)

staging_songs_copy = """
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""".format(
    config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"]
)

# FINAL TABLES

# Insert data from staging table and skip duplicate rows
# https://stackoverflow.com/questions/50644317/redshift-upsert-where-staging-has-duplicate-items
# https://docs.aws.amazon.com/redshift/latest/dg/r_WF_ROW_NUMBER.html
songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""


artist_table_insert = """
INSERT INTO artists
(
    WITH temp_songs AS (
	    SELECT ss.*, ROW_NUMBER() OVER (PARTITION BY ss.artist_id ORDER BY ss.year) as seqnum
	    FROM staging_songs ss)

    SELECT DISTINCT 
        ts.artist_id,
        ts.artist_name,
        ts.artist_location,
        ts.artist_latitude,
        ts.artist_longitude

    FROM temp_songs ts
    WHERE ts.seqnum = 1 
);
"""

time_table_insert = """
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    artist_table_create,
    user_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    artist_table_drop,
    user_table_drop,
    song_table_drop,
    time_table_drop,
    songplay_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    artist_table_insert,
    user_table_insert,
    song_table_insert,
    time_table_insert,
    songplay_table_insert,
]
