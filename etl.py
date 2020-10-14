import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour,
                                   weekofyear, date_format)
from pyspark.sql.types import (StructType, StructField, StringType,
                               DecimalType, IntegerType, TimestampType,
                               DoubleType, LongType)


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    Load the song data json files into a dataframe. Create songs table
    with desired columns, and write
    table into partitioned parquet files. Create artists table with
    desired columns and write parquet files.
    
    Parameters
    ----------
    spark: spark session
    input_data: str
        Full path to location of json input data files
    output_data: str
        Full path to location where parquet files will be written
    """
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    song_schema = StructType([
            StructField('artist_id', StringType(), True),
            StructField('artist_latitude', StringType(), True),
            StructField('artist_longitude', StringType(), True),
            StructField('artist_location', StringType(), True),
            StructField('artist_name', StringType(), True),
            StructField('duration', DoubleType(), True),
            StructField('num_songs', LongType(), True),
            StructField('song_id', StringType(), True),
            StructField('title', StringType(), True),
            StructField('year', LongType(), True)
        ])
    df =spark.read.json(song_data, schema=song_schema)
    
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration", 
                            col("artist_name").alias("artist")).distinct()                        
    songs_table.write.partitionBy("year", "artist").parquet(output_data+'songs/')
    
    artists_table = df.select("artist_id", "artist_name", "artist_location",
                              "artist_longitude",
                              "artist_latitude").dropDuplicates(['artist_id'])
    artists_table.write.parquet(output_data+'artists/')


def process_log_data(spark, input_data, output_data):
    """ 
    Load the log data json files into a dataframe. Filter out data that
    doesnt have "NextSong" as the value for the column page. Create
    users table with desired columns, and write table into parquet
    files. Create times table with desired columns and write
    partitioned parquet files. Read in song data and join
    with log data to generate the songplays fact table. Write
    partitioned parquet files for the songplays table.
    
    Parameters
    ----------
    spark: spark session
    input_data: str
        Full path to location of json input data files
    output_data: str
        Full path to location where parquet files will be written
    """
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
     
    log_schema = StructType([
        StructField('artist', StringType(), True),
            StructField('auth', StringType(), True),
            StructField('firstName', StringType(), True),
            StructField('gender', StringType(), True),
            StructField('itemInSession', LongType(), True),
            StructField('lastName', StringType(), True),
            StructField('length', DoubleType(), True),
            StructField('level', StringType(), True),
            StructField('location', StringType(), True),
            StructField('method', StringType(), True),
            StructField('page', StringType(), True),
            StructField('registration', DoubleType(), True),
            StructField('sessionId', LongType(), True),
            StructField('song', StringType(), True),
            StructField('status', LongType(), True),
            StructField('ts', LongType(), True),
            StructField('userAgent', StringType(), True),
            StructField('userId', StringType(), True),
        ])
    
    df = spark.read.json(log_data, schema=log_schema)
    df = df.filter(df.page=="NextSong")
    
    users_table = df.select("userId", "firstName", "lastName", "gender",
                            "level")
    users_table.write.parquet(output_data+'users/')
    
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)),
                       TimestampType())
    df = df.withColumn('start_time', get_datetime(df.timestamp))

    time_table = df.select('ts','start_time', hour("start_time").alias('hour'), 
                           dayofmonth("start_time").alias('day'),
                           weekofyear("start_time").alias('week'),
                           month("start_time").alias('month'),
                           year("start_time").alias('year'),
                           date_format("start_time", 'E').alias('weekday')).distinct()
    time_table.write.partitionBy("year", "month").parquet(output_data+'times/')

    song_schema = StructType([
            StructField('artist_id', StringType(), True),
            StructField('artist_latitude', StringType(), True),
            StructField('artist_longitude', StringType(), True),
            StructField('artist_location', StringType(), True),
            StructField('artist_name', StringType(), True),
            StructField('duration', DoubleType(), True),
            StructField('num_songs', LongType(), True),
            StructField('song_id', StringType(), True),
            StructField('title', StringType(), True),
            StructField('year', LongType(), True)
        ])
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json')
                              , schema=song_schema)
    songplays_table = df.join(song_df, (df.song == song_df.title) &
                              (df.artist == song_df.artist_name)
                              & (df.length == song_df.duration),
                              'left_outer').select(df.start_time,
                                                   col("userId").alias(
                                                       "user_id"),
                                                   df.level, song_df.song_id,
                                                   song_df.artist_id,
                                                   col("sessionId").alias("session_id"),
                                                   df.location,
                                                   col("userAgent").alias("user_agent"),
                                                   year("start_time").alias("year"),
                                                   month("start_time").alias("month")
                                )
    songplays_table.write.partitionBy("year", "month").parquet(output_data +
                                                               'songplays/')


def main():
    """
    Create Spark session. Define input and output data paths. 
    Create parquet files for the song data and log data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "/home/workspace/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
