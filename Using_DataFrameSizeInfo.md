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

1. Create a DataFrameSizeInfo object to hold the stats for a given DataFrame

Code:

`df_stats = DataFrameSizeInfo(df)`

Return:
>DataFrame Statistics Calculated
>
>Size in bytes: 2,454,772,600
>
