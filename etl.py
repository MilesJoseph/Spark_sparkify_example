import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] =  config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    Creating a spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Here we are writing a function to import and export data from s3 for the song data. Also, we will need to build a structure for it to be put into based on star schema.
    """
    song_data = input_data + 'song_data/*/*/*/*.json'

    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])

    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_fields = ["title", "artist_id", "year", "duration"]

    # write songs table to parquet files partitioned by year and artist
    songs_table = df.select(songs_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())

    songs_table = df.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_latitude as latitude", "artist_longitude as longitude", "artist_location as location", "artist_name as name" ]

    # write artists table to parquet files
    artists_table = df.select(artists_fields).dropDuplicates()

    artists_table = df.write.parquet(output_date + 'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_fields = ["userdId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]

    users_table = df.selectExpr(users_fields).dropDuplicates()

    # write users table to parquet files
    users_table = df.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(dateconversion, TimestampType())
    df =  df.withColumn("timestamp", get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(dateconversion, TimestampType())
    df =  df.withColumn("timedate",get_datetime('td'))

    # extract columns to create time table
    time_table = df.select("timestamp", "timedate").dropDuplicates()\
        .withColumn("hour", hour(col("timestamp")).withColumn("minute", minute(col("timestamp"))\
        .withColumn("second", second(col("timestamp")).withColumn("weekday", date_format(col("timedate"), 'E'))\
        .withColumn("month", month(col("timedate")).withColumn("year", year(col("timedate")))))))

    # write time table to parquet files partitioned by year and month
    time_table = songs_table.write.partitionBy("year", "month").parquet(output_data + 'time/')


    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/*')

    df_artists = spark.read.parquet(output_data + 'artists/*')

    songs_logs = df.join(song_df, (df.song == song_df.title))
    artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.name))

    # extract columns from joined song and log datasets to create songplays table
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.timestamp, 'left'
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month')
    )


    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
