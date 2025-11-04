# filter transformations in PySpark
df.filter(F.col("name").startswith("a")).show()
df.filter(F.col("name").endswith("n")).show()
df.filter(F.col("country").like("Ind%")).show()

# difference between drop and dropduplicates
df.distinct().show() # used to get unique rows based on all columns
df.dropDuplicates(["name"]).show() # used to get unique rows based on specific columns. Her columns needs to be passed as list
df.dropDuplicates(["name", "country"]).show() # unique rows based on name and country columns

# sort and orderby
df.sort(F.col("name").asc()).show() # sort in ascending order based on name column
df.sort(F.col("name").desc()).show() # sort in descending order based on name column
df.orderby(f.COL("COUNTRY").ASC(),F.col("continent").desc()).show() # order by multiple columns with different sort orders
df.orderby(["country","continent"],ascending=[1,0]).show() # order by multiple columns with different sort orders using list
df.orderby("country","continent").show() # order by multiple columns in ascending order by default

# groupby and aggregation
df.groupby("country").count().show() # group by country and count number of rows in each group
df.groupby("continent").agg(F.sum("population").alias("total_population")).show() # group by continent and sum population in each group
df.groupby("continent").agg(F.avg("population").alias("avg_population")).show() # group by continent and average population in each group
df.groupby("continent").agg(F.max("population").alias("max_population")).show() # group by continent and get max population in each group
df.groupby("continent").agg(F.min("population").alias("min_population")).show() # group by continent and get min population in each group
df.groupby("continent").agg(F.collect_list("country").alias("countries")).show() # group by continent and collect list of countries in each group
df.groupby("continent").agg(F.collect_set("country").alias("unique_countries")).show() # group by continent and collect set of unique countries in each group
df.groupby("continent").agg(F.countDistinct("country").alias("unique_country_count")).show() # group by continent and count distinct countries in each group
df.groupby("continent").agg(F.first("country").alias("first_country")).show() # group by continent and get first country in each group
df.groupby("continent").agg(F.last("country").alias("last_country")).show() # group by continent and get last country in each group
df.groupby("continent").agg(F.stddev("population").alias("stddev_population")).show() # group by continent and get standard deviation of population in each group
df.groupby("continent").agg(F.variance("population").alias("variance_population")).show() # group by continent and get variance of population in each group
df.groupby("continent").agg(F.sumDistinct("population").alias("sum_distinct_population")).show() # group by continent and sum distinct population in each group
df.groupby("continent").agg(F.expr("percentile_approx(population, 0.5)").alias("median_population")).show() # group by continent and get median population in each group
df.groupby("continent").agg('salary': 'avg', 'age': 'max').show() # group by continent and get average salary and max age in each group

# join operations
df1.join(df2, on="id", how="inner").show() # inner join on id column
df1.join(df2, on="id", how="left").show() # left join on id column
df1.join(df2, on="id", how="right").show() # right join on id column
df1.join(df2, on="id", how="full").show() # full outer join on id column
df1.join(df2, on=["id", "name"], how="inner").show() # inner join on multiple columns
df1.alias("a").join(df2.alias("b"), on=F.col("a.id") == F.col("b.id"), how="inner").show() # inner join using aliases
df1.crossJoin(df2).show() # cross join
df1.join(df2, on="id", how="semi").show() # semi join
df1.join(df2, on="id", how="anti").show() # anti join
df1.join(df2, on="id", how="left_semi").show() # left semi join
df1.join(df2, on="id", how="left_anti").show() # left anti join
df1.join(df2, on="id", how="natural").show() # natural join
df1.join(df2, on="id", how="using").show() # using join
df1.join(df2, on="id", how="broadcast").show() # broadcast join
df1.join(df2, on="id", how="shuffle_hash").show() # shuffle hash join
df1.join(df2, on="id", how="shuffle_sort_merge").show() # shuffle sort merge join
df1.join(df2, on="id", how="bucketed").show() # bucketed join
df1.join(df2, on="id", how="skewed").show() # skewed join
df1.join(df2, on="id", how="replicated").show() # replicated join
df1.join(df2, on="id", how="partitioned").show() # partitioned join
df1.join(df2, on="id", how="coalesce").show() # coalesce join
df1.join(df2, on="id", how="repartition").show() # repartition join
df1.join(df2, on="id", how="sort_merge").show() # sort merge join
df1.join(df2, on="id", how="hash").show() # hash join
df1.join(df2, on="id", how="map_side").show() # map side join
df1.join(df2, on="id", how="reduce_side").show() # reduce side join
df1.join(df2, on="id", how="streaming").show() # streaming join
df1.join(df2, on="id", how="temporal").show() # temporal join
df1.join(df2, on="id", how="spatial").show() # spatial join
df1.join(df2, on="id", how="fuzzy").show() # fuzzy join
df1.join(df2, on="id", how="approximate").show() # approximate join
df1.join(df2, on="id", how="windowed").show() # windowed join
df1.join(df2, on="id", how="time_travel").show() # time travel join
df1.join(df2, on="id", how="delta").show() # delta join
df1.join(df2, on="id", how="iceberg").show() # iceberg join
df1.join(df2, on="id", how="hudi").show() # hudi join
df1.join(df2, on="id", how="zorder").show() # zorder join
df1.join(df2, on="id", how="data_skipping").show() # data skipping join
df1.join(df2, on="id", how="adaptive").show() # adaptive join
df1.join(df2, on="id", how="dynamic").show() # dynamic join
df1.join(df2, on="id", how="static").show() # static join
df1.join(df2, on="id", how="multi_way").show() # multi way join
df1.join(df2, on="id", how="self").show() # self join
df1.join(df2, on="id", how="hierarchical").show() # hierarchical join
df1.join(df2, on="id", how="temporal_spatial").show() # temporal spatial join
df1.join(df2, on="id", how="graph").show() # graph join
df1.join(df2, on="id", how="ml_based").show() # ml based join
df1.join(df2, on="id", how="custom").show() # custom join

# union and unionall
df1.union(df2).show() # union of two dataframes (removes duplicates)
df1.unionAll(df2).show() # union all of two dataframes (keeps duplicates)
df1.unionByName(df2).show() # union by name of two dataframes (removes duplicates based on column names)
df1.unionByName(df2, allowMissingColumns=True).show() # union by name of two dataframes allowing missing columns (removes duplicates based on column names)
df1.unionByName(df2, allowMissingColumns=False).show() # union by name of two dataframes not allowing missing columns (removes duplicates based on column names)
df1.unionAllByName(df2).show() # union all by name of two dataframes (keeps duplicates based on column names)
df1.unionAllByName(df2, allowMissingColumns=True).show() # union all by name of two dataframes allowing missing columns (keeps duplicates based on column names)
df1.unionAllByName(df2, allowMissingColumns=False).show() # union all by name of two dataframes not allowing missing columns (keeps duplicates based on column names)
df1.select("id", "name").union(df2.select("id", "name")).show() # union of selected columns from two dataframes

# fillna and dropna
df.fillna(0).show() # fill null values with 0
df.fillna("unknown").show() # fill null values with 'unknown'
df.fillna({"age": 0, "name": "unknown"}).show() # fill null values with different values for different columns
df.dropna().show() # drop rows with any null values
df.dropna(how="all").show() # drop rows with all null values
df.dropna(thresh=2).show() # drop rows with less than 2 non-null values
df.dropna(subset=["age", "name"]).show() # drop rows with null values in specific columns
df.na.fill(0).show() # fill null values with 0 using na functions
df.na.fill("unknown").show() # fill null values with 'unknown' using na functions
df.na.fill({"age": 0, "name": "unknown"}).show() # fill null values with different values for different columns using na functions
df.na.drop().show() # drop rows with any null values using na functions
df.na.drop(how="all").show() # drop rows with all null values using na functions
df.na.drop(thresh=2).show() # drop rows with less than 2 non-null values using na functions
df.na.drop(subset=["age", "name"]).show() # drop rows with null values in specific columns using na functions
df.replace("old_value", "new_value").show() # replace old_value with new_value in the dataframe
df.replace({"old_value1": "new_value1", "old_value2": "new_value2"}).show() # replace multiple old values with new values in the dataframe
df.replace(["old_value1", "old_value2"], ["new_value1", "new_value2"]).show() # replace multiple old values with new values using lists
df.replace(to_replace="old_value", value="new_value", subset=["column1", "column2"]).show() # replace old_value with new_value in specific columns

# collect
# collect is an action that retrieves all the data from the DataFrame to the driver node as a list of Row objects.
It should be used with caution as it can lead to out of memory errors if the DataFrame is too large.
data = df.collect() # collect all rows as a list of Row objects
for row in data:
    print(row)
    print(data) # print the collected data
    print(row['column_name']) # access specific column value from Row object

