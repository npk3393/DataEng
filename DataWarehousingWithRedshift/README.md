# Data Warehousing using Amazon Redshift

## Executive Summary

Sparkify (fictitious music streaming company with 280 gigabytes of JSON documents on subscribers and their activities), has grown their user base and song database and want to move their processes and data onto the cloud. 

Data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I have built an ETL pipeline that extracts their data from Amazon S3, stages them in Amazon Redshift, and transforms data into a data mart (star schema - set of dimensional tables and a fact table) for their analytics team to continue finding insights in what songs their users are listening to. 

## Important Note

personally identifiable information and data sets are not available in this repository

## Project Datasets

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

## Data Mart Model

### Dimension Tables
1. users - users in the app
    user_id, first_name, last_name, gender, level
2. songs - songs in music database
    song_id, title, artist_id, year, duration
3. artists - artists in music database
    artist_id, name, location, lattitude, longitude
4. time - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday

### Fact Table
Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Project Files

- create_table.py is where I created my fact and dimension tables for the star schema in Amazon Redshift.
- etl.py is where I had loaded data from Amazon S3 into staging tables on Redshift and then processed that data into my  analytics tables on Amazon Redshift.
- sql_queries.py is where I had defined SQL statements, which will be imported into the two other files above.
- README.md is where I provided discussion on my process and decisions for this ETL pipeline.

## Data Modeling and ETL Pipelines 

Due to the nature of the data set, my design approach is to keep it simple. I utilized a star schema with one fact table and four dimensional tables with fact table containing userId as the distribution key and a compound sorting key using time stamp and user Id.

Compound Sort Keys are made up of all the columns that are listed in the Redshift sort keys definition during the creation of the table, in the order that they are listed. Based on my initial observations and data analysis (out of scope for this project), I chose time stamp as it being the most frequently used, closely followed by userId (to obtain user information and obtianing listening patterns during a given time interval).

My compound sort keys design can speed up joins, GROUP BY and ORDER BY operations, and window functions that use PARTITION BY.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Reference

-https://www.fernandomc.com/posts/redshift-epochs-and-timestamps/

-https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html

-https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html

-https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html

-https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-authorize-cluster-access.html

-https://hevodata.com/blog/redshift-distribution-keys/

-https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_TABLE.html
