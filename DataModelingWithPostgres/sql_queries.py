# DROP TABLES

songplay_table_drop = " DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays(
songplay_id SERIAL PRIMARY KEY,
start_time timestamp not null,
user_id int not null,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
user_id int primary key,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
song_id varchar primary key,
title varchar,
artist_id varchar,
year int,
duration int
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id varchar primary key,
name varchar,
location varchar,
latitude float,
longtitude float
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time timestamp primary key,
hour int,
day int,
week int,
month int,
year int,
weekday varchar
);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent
) values (%s,%s,%s,%s,%s,%s,%s,%s)
on conflict do nothing
""")

user_table_insert = ("""INSERT INTO users(
user_id,
first_name,
last_name,
gender,
level) 
values (%s,%s,%s,%s,%s)
on conflict (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs(
song_id,
title,
artist_id,
year,
duration
) values (%s,%s,%s,%s,%s)
on conflict do nothing
""")

artist_table_insert = ("""INSERT INTO artists(
artist_id,
name,
location,
latitude,
longtitude
) values (%s,%s,%s,%s,%s)
on conflict do nothing
""")


time_table_insert = ("""INSERT INTO time(
start_time,
hour,
day,
week,
month,
year,
weekday
) values (%s,%s,%s,%s,%s,%s,%s)
on conflict do nothing
""") 

# FIND SONGS

song_select = ("""
select songs.song_id, artists.artist_id 
from songs
inner join artists 
on songs.artist_id = artists.artist_id
where songs.title=%s and 
artists.name = %s and
songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]