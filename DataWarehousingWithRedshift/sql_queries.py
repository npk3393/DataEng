import configparser

# the schema for staging tables that you see are repurposed from Postgres project

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# initializing the parameters needed for the ETL job

DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")
ARN = config.get("IAM_ROLE", "ARN")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")


# DROP TABLES
# Dropping tables so everytime I troubleshoot I can recreate instead of appending or overwriting

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
# Creating staging tables

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS 
staging_events (
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession integer,
    lastName text,
    length float4,
    level text,
    location text,
    method text,
    page text,
    registration float8,
    sessionId integer,
    song text,
    status integer,
    ts bigint,
    userAgent text,
    userId text
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS 
staging_songs (
    num_songs integer,
    artist_id text,
    artist_lattitude float4,
    artist_longitude float4,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration float4,
    year integer
);
""")

# FACT TABLE
# Creating the fact table with distribution key and compound sort key

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS 
songplay (
    songplay_id integer IDENTITY(0,1),
    start_time timestamp,
    user_id text,
    level text,
    song_id text,
    artist_id text,
    session_id integer,
    location text, 
    user_agent text,
    primary key(songplay_id),
    foreign key(user_id) references users(user_id),
    foreign key(artist_id) references artists(artist_id),
    foreign key(song_id) references songs(song_id),
    foreign key(start_time) references time_table(start_time)
)

    distkey(user_id) 
    compound sortkey(start_time, user_id);

""")

# DIMENSION TABLE
# Creating the dimension tables for the data mart

user_table_create = ("""
CREATE TABLE IF NOT EXISTS 
users (
    user_id text, 
    first_name text, 
    last_name text, 
    gender text, 
    level text,
    primary key(user_id)
)
    diststyle even
    sortkey(user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS 
songs (
    song_id text, 
    title text, 
    artist_id text, 
    year integer, 
    duration float4,
    primary key(song_id)
)
    diststyle even
    sortkey(song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS 
artists (
    artist_id text, 
    name text, 
    location text, 
    lattitude float4, 
    longitude float4,
    primary key(artist_id)
)
    diststyle even
    sortkey(artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS 
time_table (
    start_time timestamp, 
    hour integer, 
    day integer, 
    week integer, 
    month integer, 
    year integer, 
    weekday text,
    primary key(start_time)
)
    diststyle even
    sortkey(start_time);
""")

# LOADING THE STAGING TABLES 
# Loading the staging tables from amazon s3
# A lot of code is not available that are associated with this process due to my credentials closely associated with it.

staging_events_copy = ("""
copy staging_events 
from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role=arn:aws:iam::067688425893:role/dwhRole'
region 'us-west-2'
format as json {};

""").format(LOG_JSONPATH)
# credentials 'aws_iam_role={}''


staging_songs_copy = ("""
copy staging_songs 
from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role=arn:aws:iam::067688425893:role/dwhRole'
region 'us-west-2'
format as json 'auto';
""").format()

# LOADING THE DATA MART 
# Loading the data mart tables which are the four dimension tables and one fact table

songplay_table_insert = ("""

insert into songplay(start_time, user_id, level, song_id, artist_id,
session_id, location, user_agent)

select distinct 
    timestamp 'epoch' + (staging_events.ts/1000) * interval '1 second' as start_time,
    staging_events.userId,
    staging_events.level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.sessionId,
    staging_events.location,
    staging_events.userAgent
from 
    staging_songs
join 
    staging_events 
on
    (staging_events.artist = staging_songs.artist_name AND
    staging_events.song = staging_songs.title)
where 
    staging_events.page = 'NextSong';

""")

user_table_insert = ("""
insert into users(user_id , first_name , last_name , gender , level)
select distinct 
    userId, 
    firstName, 
    lastName, 
    gender, 
    level 
from 
    staging_events
where 
    userId is not null and 
    page = 'NextSong';
""")

song_table_insert = ("""
insert into songs(song_id , title , artist_id , year , duration)
select distinct 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration 
from 
    staging_songs
where 
    song_id is not null;

""")

artist_table_insert = ("""
insert into artists(artist_id , name , location , lattitude , longitude)
select distinct 
    artist_id, 
    artist_name, 
    artist_location, 
    artist_lattitude, 
    artist_longitude
from 
    staging_songs
where 
    artist_id is not null;


""")

time_table_insert = ("""
insert into time_table(start_time, hour , day , week , month , year , weekday)
select distinct 
    timestamp 'epoch' + (ts/1000) * interval '1 second' as start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(dayofweek from start_time) as weekday
from 
    staging_events;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
