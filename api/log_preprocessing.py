# %%
"""
# Log File - Data Exploration
"""

# %%
"""
## Loading Libraries
"""

# %%
from pyspark.sql import SparkSession

# %%
"""
## Spark Session Object Creation
Avoid org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading of Spark 3.0
"""

# %%
spark = SparkSession\
.builder\
.appName("pyspark-notebook").\
config("spark.sql.legacy.timeParserPolicy", "LEGACY").\
getOrCreate()

# %%
"""
## Loading
"""

# %%
log_file_path="actual_log.txt"

# %%
base_df = spark.read.text(log_file_path)
# Let's look at the schema
base_df.printSchema()
base_df.show(truncate=False)

# %%
"""
## Parsing
"""

# %%
"""
If you're familiar with web servers at all, you'll recognize that this is in <a href="https://www.w3.org/Daemon/User/Config/Logging.html#common-logfile-format"> Common Log Format </a>. The fields are:

<b>remotehost rfc931 authuser date "request" status bytes</b>

<table>
    <tr>
        <th>field</th>
        <th>meaning</th>
    </tr>
    <tr>
        <td>remotehost</td>
        <td>Remote hostname (or IP number if DNS hostname is not available).</td>
    </tr>
    <tr>
        <td>rfc931</td>
        <td>The remote logname of the user. We don't really care about this field.</td>
    </tr>
    <tr>
        <td>authuser</td>
        <td>The username of the remote user, as authenticated by the HTTP server.</td>
    </tr>
    <tr>
        <td>date</td>
        <td>The date and time of the request.</td>
    </tr>
    <tr>
        <td>request</td>
        <td>The request, exactly as it came from the browser or client.</td>
    </tr>
    <tr>
        <td>status</td>
        <td>The HTTP status code the server sent back to the client.</td>
    </tr>
    <tr>
        <td>bytes</td>
        <td>The number of bytes (Content-Length) transferred to the client.</td>
    </tr>
</table>
Next, we have to parse it into individual columns. We'll use the special built-in regexp_extract() function to do the parsing. This function matches a column against a regular expression with one or more capture groups and allows you to extract one of the matched groups. We'll use one regular expression for each field we wish to extract.
<br> </br>   
If you feel regular expressions confusing, and you want to explore more about them, start with the <a href="https://regexone.com/">RegexOne web site</a>.
"""

# %%
from pyspark.sql.functions import split, regexp_extract
split_df = base_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                          regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                          regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
                          regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                          regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size'))
split_df.show(truncate=False)

# %%
"""
## Cleaning
Let's see how well our parsing logic worked. First, let's verify that there are no null rows in the original data set.
"""

# %%
base_df.filter(base_df['value'].isNull()).count()

# %%
"""
That means there is no line with null values. Lets check the parsed dataframe
"""

# %%
bad_rows_df = split_df.filter(split_df['host'].isNull() |
                              split_df['timestamp'].isNull() |
                              split_df['path'].isNull() |
                              split_df['status'].isNull() |
                             split_df['content_size'].isNull())
bad_rows_df.count()

# %%
"""
Some of the fields are not populated in the original data. We need to find out which fields are affected.
"""

# %%
from pyspark.sql.functions import col, sum

def count_null(col_name):
  return sum(col(col_name).isNull().cast('integer')).alias(col_name)
exprs = []
[exprs.append(count_null(col_name)) for col_name in split_df.columns]
split_df.agg(*exprs).show()

# %%
"""
## Fix the rows with null content_size
"""

# %%
"""
The easiest solution is to replace the null values in split_df with 0. The DataFrame API provides a set of functions and fields specifically designed for working with null values. We use na to replace <code>content_size</code> with 0.
"""

# %%
cleaned_df = split_df.na.fill({'content_size': 0})
exprs = []
[exprs.append(count_null(col_name)) for col_name in cleaned_df.columns]

cleaned_df.agg(*exprs).show()

# %%
from pyspark.sql.functions import *
logs_df = cleaned_df.select('*', to_timestamp(cleaned_df['timestamp'],"dd/MMM/yyyy:HH:mm:ss ZZZZ").cast('timestamp').alias('time')).drop('timestamp')
total_log_entries = logs_df.count()
print(total_log_entries)
logs_df.show(truncate=False)

# %%
#01/Aug/1995:00:00:07 -0400
cleaned_df.select('*').withColumn('time',to_timestamp(col("timestamp"),'dd/MMM/yyyy:HH:mm:ss ZZZZ')) \
  .show(truncate=False)