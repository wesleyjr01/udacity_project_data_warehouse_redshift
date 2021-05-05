import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stating_songs"
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

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

songplay_table_create = """
"""

user_table_create = """
"""

song_table_create = """
"""

artist_table_create = """
"""

time_table_create = """
"""

# STAGING TABLES

staging_events_copy = (
    """
"""
).format()

staging_songs_copy = (
    """
"""
).format()

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
