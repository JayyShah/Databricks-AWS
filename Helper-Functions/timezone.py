"""

Problem Statement : Write a helper function which returns a Dataframe, containinig DLST switches(in local time) 
for a certain timezone with offset. / Getting the correct timezone offset using local timezone.

1. Implementing a Python function to get a timezone 

"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType
import pytz
import datetime

def get_timezone_offset(tz_list, start_date, end_date):
    # Create Spark Session
    spark = SparkSession.builder.appName("Timezone_offset").getOrCreate()
    
    # Convert the input dates to datetime objects
    start_date = datetime.datetime.strptime(start_date, '%d.%m.%Y %H:%M:%S')
    end_date = datetime.datetime.strptime(end_date, '%d.%m.%Y %H:%M:%S')
    
    # Define the UDF to convert DLST switches to Timestamp
    def convert_to_timestamp(x):
        return datetime.datetime.strptime(x, '%d.%m.%Y %H:%M:%S')

    convert_to_timestamp_udf = udf(convert_to_timestamp, TimestampType())
    
    # Create the dataframe
    df = spark.createDataFrame(tz_list, ["TZ", "DLST_FROM", "DLST_TO", "TZ_OFFSET"])
    df = df.withColumn("DLST_FROM", convert_to_timestamp_udf(df["DLST_FROM"]))
    df = df.withColumn("DLST_TO", convert_to_timestamp_udf(df["DLST_TO"]))
    
    # Filter the dataframe for the given time range
    df = df.filter((df["DLST_FROM"] >= start_date) & (df["DLST_TO"] <= end_date))
    
    return df

# Example usage
tz_list = [("Europe/Berlin", "28.03.2010 02:00:00", "31.10.2010 02:00:00", "+02")]
get_timezone_offset(tz_list, "01.01.2010 02:00:00", "01.01.2024 02:00:00").show()



# ---------------------------------------------------------------------------------------------------------------



"""

Approach 2: Fetching the data from thr Oracle Database

"""

from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName("DLST Switches").getOrCreate()

# load data from Oracle table
df = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:oracle:thin:@host:port:sid") \
  .option("dbtable", "table_name") \
  .option("user", "user") \
  .option("password", "password") \
  .load()

# filter data by TZ and date range
def get_dlst_switches(df, tz, start_date, to_date):
  return df \
    .filter(df["TZ"] == tz) \
    .filter(df["DLST_FROM"] >= start_date) \
    .filter(df["DLST_TO"] <= to_date)

# create a UDF to convert DLST switches to Timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType

convert_to_timestamp = udf(lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M:%S'), TimestampType())

# apply the UDF to DLST_FROM and DLST_TO columns
df = df.withColumn("DLST_FROM", convert_to_timestamp(df["DLST_FROM"]))
df = df.withColumn("DLST_TO", convert_to_timestamp(df["DLST_TO"]))

# return the filtered dataframe with Timestamp columns
result = get_dlst_switches(df, "Europe/Berlin", "28.03.2010 02:00:00", "31.10.2010 02:00:00")





# ---------------------------------------------------------------------------------------------------------------------




"""

Approach 3: Reading data using JDBC connector, from any database

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder.appName("Timezone_Offset").getOrCreate()

# Read data from database using JDBC connector
jdbc_url = "jdbc:oracle:thin:@<hostname>:<port>:<sid>"

table_name = "<table_name>"

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .load()

# Create a UDF to convert DLST_FROM and DLST_TO columns to Timestamp
convert_to_timestamp = F.udf(lambda x: F.to_utc_timestamp(x, "dd.MM.yyyy HH:mm:ss"))
df = df.withColumn("DLST_FROM", convert_to_timestamp(df.DLST_FROM))
df = df.withColumn("DLST_TO", convert_to_timestamp(df.DLST_TO))

# Helper function to get the correct timezone offset using local timezone
def get_timezone_offset(tz, start_date, end_date):
    df_filtered = df.filter((df.TZ == tz) & (df.DLST_FROM >= start_date) & (df.DLST_TO <= end_date))
    return df_filtered

# Call the helper function
get_timezone_offset("Europe/Berlin", "2010-03-28 02:00:00", "2011-03-27 02:00:00").show()
