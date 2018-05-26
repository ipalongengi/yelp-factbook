# What is spark?
* Spark is an open-source powerful distributed querying and processing engine.
* It provides flexibility and extensibility of MapReduce but at significantly higher speeds
* Spark allows the user to read, transform, and aggregate data, as well as train and deploy sophisticated statistical models with ease.
* Any Spark application spins off a single driver process on a *master* node
	* The **Master** node then directs executor processes distributed to a number of *worker* nodes.

# Resilient Distributed Dataset
* RDD is a distributed collection of immutable Java Virtual Machine (JVM) objects.
* RDD's are objects that allow any job to perform calculations very quickly
* RDDs are calculated against, cached, and stored in-memory: a schema that results in orders of magnitude faster computations compared to other traditional distributed frameworks that Apaache Hadoop.
* RDD's expose some coarse-grained transformations such as map, reduce, and filter
* RDD's apply and log transformation to the data in parallel, result in both increased sped and fault-tolerance
* RDD's provide a data lineage - a form of an ancestry tree for each intermediate step in the form of a graph. In effect, this guards the RDDs against data loss.
* RDDs have two sets of parallel operations: **transformations** which return pointers to new RDD and **actions** which return values to the driver after running a computation.

# DataFrames
* DataFrames, like RDDs, are immutable collections of data distributed among the nodes in a cluster. However, in DataFrames data is organized into named columns.
* DataFrames were designed to make large data sets processing even easier. They allow developers to formalize the structure of the data, allowing higher-level abstraction; in that sense DataFrames resemble tables and therefore we can use SQL commands, theyre like database tables.

# SparkSession
* SparkSessions is essentially a combination of Spark context, SQL context, and Hive context.
* It allows you to replace:
	df = sql.Context.read \
		.format('json').load('py/test/sql/people.json')
* Now you can write:
	df = spark.read.format('json').load('py/test/sql/people.json')
or
	df = spark.read.json('py/test/sql/people.json')
