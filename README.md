## Project Description

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# First Steps

The first thing that we will need to do is setup our access and secret access AWS key in the file;

dl.cfg

Then we will need to bring those keys to our environment

os.environ['AWS_ACCESS_KEY_ID'] =  config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


# Creating a spark session


The next thing we will need to do is create our spark session, we do so;

def create_spark_session():
    """
    Creating a spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


# Functions

The next thing we need to do is define functions for both reading and writing the data from the s3 udacity bucket. Importantly we also want to define the structure that these records will be inserteed to. This project calls for star schema. We create the structure for the song data;

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



Since the schema in this first function (process_song_data) will compose our dim data we also want to make sure that we are dropping duplicate values from the records, pretty straightforward.

After we have defined the input data, transformed it to adhere to the overall start schema, then we will write it back to s3 via song data.

We are going to do this again for the log data with the difference being that in addition to some dim tables, we are also creating a songplays table.

The songplays table is joined to some of our other dim tables for data enrichment.
