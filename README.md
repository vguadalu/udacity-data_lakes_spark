# Sparkify Data Lake ETL

### Purpose
The purpose of this project is to grab Sparkify data located in S3 buckets and trnasform them into dimensional tables stored. This will allow the Sparkify team to easily access their data.

### Schema
The schema used for this project is a star schema where the fact table is the songplays table. The songplays table contains the songplay id, timestamp, user id, artist id, level, song_id, location, session id and user agent. 

There are four dimension tables, users, artists, times, and songs. The users table contains the user id, first name, last name, gender and level. The songs table contains the song id, title, arist id, year and duration. The times table contains the start time, hour, day, week, month, year and weekday. 

### ETL
The data files for the database are stored in S3 buckets. The data was extracted by using the pyspark read.json command and createing a dataframe. Once all the data was in dataframes, the desired data for each table in the star schema was populated by selecting the desired columns. Once the tables were populated each table was written into parquet files. The time, song and songplays tables were partitioned before being written to the parquet files. The songs data was paritioned by the year and atrist. The time and songplays data was partitioned by using the year and month columns.


### Contents of Package
The following are the files contained in this package:
- dl.cfg: Configuration files with AWS credentials
- etl.py: Python script which extracts, transforms and inserts the data from all the JSON files located in data/ into the tables of the Sparkify databse.
- README.md: ReadMe currently reading, which contains the purpose and implementation of the Sparkify database, content of the package, and how to generate the database.

### How generate parqute files
To generate the parquet files for each table the following step should be followed:
1. Update dl.cfg with AWS Access Key Id and Access Secret Key
2. Insert data into the tables:
    python etl.py
