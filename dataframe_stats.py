"""
Helpful classes and methods for working with dataframes.

When working in spark, it is important to stay aware of the size
of your dataframes, how many partitions they are on, and how
uniform the data is spread across the partitions.
"""

from pyspark import StorageLevel
from pyspark.sql.dataframe import DataFrame


class DataFrameSizeInfo():
    """
    DataFrameSizeInfo calculates and tracks the size and number
    of partitions for a given dataframe.

    Input:
        df: DataFrame
    
    Usage examples:
        df_size_info = DataFrameSizeInfo(df):
            Creates a DataFrameSizeInfo object for the given dataframe.
        df_size_info.print_stats():
            Prints the dataframe's size in bytes, number of partitions, and
            average bytes per partition.
        df_size_info.recalc_stats():
            Recalculates the dataframe's size in bytes, number of partitions,
            and average bytes per partition.
    """
    
    # Optimal Spark partition size ~128MB = 128 * 2^20 = 134,217,728 bytes
    target_partition_bytes = 134217728.0

    def __init__(self, df: DataFrame):
        """
        Initialize the DataFrameSizeInfo Object

        Calculate and print the dataframe statistics
        """
        self.df = df.select("*")
        self.df.persist(StorageLevel.MEMORY_ONLY)
        self.df.count()  # Force the dataframe to be persisted
        self._is_persisted = True

        self._calculate_stats()
        
        self.df.unpersist()
        self._is_persisted = False

        print("DataFrame Statistics Calculated\n")

        self.print_stats()
    
    def print_stats(self):
        """Print the gathered dataframe statistics"""
        print(f"Size in bytes: {self.size_in_bytes:,}")
        print(f"Number of partitions: {self.num_partitions:,}")
        print(
            f"Average bytes per partition: {self.avg_bytes_per_partition:,.2f}"
        )
        print(
            f"Recommended partition count: {self.recommended_partition_count}"
        )
    
    def _calculate_stats(self):
        """Calculate stats"""
        if self._is_persisted:
            self.size_in_bytes = self.df._jdf.queryExecution() \
                .optimizedPlan().stats().sizeInBytes()
            self.num_partitions = self.df.rdd.getNumPartitions()
            self.avg_bytes_per_partition = \
                self.size_in_bytes / self.num_partitions
            self.recommended_partition_count = \
                int(self.size_in_bytes / self.target_partition_bytes) + 1
    
    def recalc_stats(self):
        """Recalculate the stats for the provided dataframe"""
        self.df.persist(StorageLevel.MEMORY_ONLY)
        self.df.count()  # Force the dataframe to be persisted
        self._is_persisted = True

        self._calculate_stats()
        
        self.df.unpersist()
        self._is_persisted = False

        print("DataFrame Statistics Recalculated\n")

        self.print_stats()


class DataFrameSizeInfoExtra(DataFrameSizeInfo):
    """
    DataFrameSizeInfoExtra inherits from DataFrameSizeInfo

    Adds some additional details but can take > 14x as long
    as DataFrameSizeInfo. Recommended to only use this version
    of the class if you need the extra details around data rows
    per partition.
    """

    def _calculate_stats(self):
        super()._calculate_stats()
        if self._is_persisted:
            self._row_counts_per_partition = \
                self.df.rdd.mapPartitions(lambda x: [len(list(x))]).collect()
            self._row_counts_per_partition.sort()
    
    def print_rows_per_partition(self):
        for row in self._row_counts_per_partition:
            print(f"{row:,}")

    def print_stats(self):
        super().print_stats()
        print("Rows per partition:")
        self.print_rows_per_partition()
