import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stating_songs"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
    staging_events_id INT IDENTITY(1,1),
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
    user_id INT);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS stating_songs(
    staging_songs_id INT IDENTITY(1,1),
    num_songs INT,
    artist_id VARCHAR(50),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(50),
    title VARCHAR(255),
    duration FLOAT,
    year INT);
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
COPY stating_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""".format(
    config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"]
)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
