# Using the DataFrameSizeInfo Class

## Introduction

The DataFrameSizeInfo class provides byte size and partition count details
about a given DataFrame. When working with DataFrames in Spark, it is important
to stay aware of the state of the partitioned data.

The in-memory partitioning is important to pay attention to for many reasons,
a few of which are:
- Spark likes to operate on partitions that are close to 128MB. This is not so
small that you have to worry about the overhead of managing too many data partitions,
and it's not so big that every operation on the partitions takes too long.
- When doing a series of transformations on a given DataFrame, you can run
into a situation where some partitions have way more or way less data than
other partitions. This can potentially lead to downstream operations being
held up by the oversized partitions taking much longer to process than others. You
have the ability to repartition DataFrames to redisperse data evenly across the
number of partitions you specify.
- In-memory partitioning effects the number and size of files that are written
when saving a DataFrame to disk. For the same reason as the previous point,
you could end up having files saved to disk that are of uneven sizes, which
can lead to differing read times when trying to use those files downstream.

Unfortunately, as of the writing of this document, there is no easy method
to call on a DataFrame to get the necessary information to optimize the
partitioning (although, Spark has come a long way in recent years to try to
optimize partitioning when reading from large files, etc.). That said, there
are ways to find the information needed to repartition DataFrames to increase
efficiency.

This is where the DataFrameSizeInfo class comes into play. This class will
find the relevant information about a DataFrame to highlight if the
partitioning looks good or not, and will provide a recommendation of
number of partitions to use to get your average partition size closer
to 128MB.

## DataFrameSizeInfo details

The way the DataFrameSizeInfo class works is that it first makes a copy of
the DataFrame you are interested in, and it persists that DataFrame to memory.
Once persisted, the class will identify the byte size of the DataFrame as well
as the number of partitions it is on. If it is important to know how many of the
data rows are on each of the partitions, the DataFrameSizeInfoExtra sub-class
adds functionality to determine that information as well.

### Example code: calculate DataFrame byte size

To calculate the byte size of the DataFrame, the underlying _jdf (java DataFrame)
of the PySpark DataFrame is accessed:

`df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()`

### Example code: calculate the number of partitions for the DataFrame

To calculate the number of partitions that the DataFrame is sitting on, the
underlying RDD (Resilient Distributed Dataset) of the PySpark DataFrame
is accessed:

`df.rdd.getNumPartitions()`

### Example code: getting the row count for each partition (in DataFrameSizeInfoExtra)

To get the per-partition row count, a lambda function is passed to each partition
of the RDD to get the number of rows in each partition

`df.rdd.mapPartitions(lambda x: [len(list(x))]).collect()`

## Using DataFrameSizeInfo

1. Get the DataFrameSizeInfo class into the Databricks Notebook where you
want to use it

There are multiple ways to import helpful functions and classes into a Databricks
Notebook. One way is to copy the code into a cell in the notebook. This may be
easier in one-off situations. But it is much nicer to be able to run one line
of code to make the class available.

If you have the class stored in another notebook in your Databricks workspace,
a nicer way to access the class is through the %run command.

You would use the following code in a cell with just this one line:

`%run /Workspace/path/to/class/notebook`

2. Create a DataFrameSizeInfo object to hold the stats for a given DataFrame

Code:

`df_stats = DataFrameSizeInfo(df)`

Return:

>DataFrame Statistics Calculated
>
>Size in bytes: 2,454,772,600<br>
>Number of partitions: 12<br>
>Average bytes per partition: 204,564,383.33<br>
>Recommended partition count: 19

3. If there is a need to see the row counts per
partition, use DataFrameSizeInfoExtra

Code:

`df_stats = DataFrameSizeInfoExtra(df)`

Return:

>DataFrame Statistics Calculated
>
>Size in bytes: 2,454,772,600<br>
>Number of partitions: 12<br>
>Average bytes per partition: 204,564,383.33<br>
>Recommended partition count: 19<br>
>Rows per partition:<br>
>0<br>
>982,928<br>
>1,599,455<br>
>1,735,425<br>
>1,735,469<br>
>1,816,638<br>
>1,926,110<br>
>2,112,446<br>
>2,370,310<br>
>2,370,620<br>
>2,370,630<br>
>2,370,630

4. Use the recommended partition count to repartition the DataFrame

Code:

`df_repartitioned = df.repartition(df_stats.recommended_partition_count)`

5. Creating a new DataFrameSizeInfo object based on this new repartitioned
DataFrame shows the average bytes per partition have been optimized to be
closer to 128MB (134,217,728 bytes)

Code:

`df_repartitioned_stats = DataFrameSizeInfo(df_repartitioned)`

Return:

>DataFrame Statistics Calculated
>
>Size in bytes: 2,454,772,600<br>
>Number of partitions: 19<br>
>Average bytes per partition: 129,198,557.89<br>
>Recommended partition count: 19

## Conclusion

It is important to stay aware of the size of your DataFrame and repartition
if the number of partitions is to high or too low, or if the amount of
data on each partition is not uniform across the DataFrame.

The purpose of the DataFrameSizeInfo class is to help make the investigation
and effective repartitioning of a DataFrame easier.
