# Large Scale Geo-Spatial Data Analysis using Apache Spark and SparkSQL

## Distributed and Parallel Database Systems

## Project description

The project was aimed to setup a spark cluster with HDFS and run SparkSQL queries (geo-spatial) on the it.

**Input:** A collection of New York City Yellow Cab taxi trip records spanning January 2009 to June 2015. The source data may be clipped to an envelope encompassing the five New York City boroughs in order to remove some of the noisy error data (e.g., latitude 40.5N – 40.9N, longitude 73.7W – 74.25W).

* Native spark cluster was used as cluster manager.
* Hadoop Distributed File System (HDFS) was used as distributed storage system.
* The setup was done using Amazon EC2 virtual machines as nodes.
* Spatial queries such as range query, range join query, distance query, distance join query, hot zone analysis and hot cell analysis were executed.
  - Spatial queries were executed by implementing user defined functions such as ST_contains and ST_within in Scala.
  - ST_contains takes a point and a rectangle and returns a boolean indicating whether the point is inside the rectangle.
  - ST_within takes two points and a distance and returns a boolean indication whether the distance between the points is not more than the distance provided.

**Output:** A list of the fifty most significant hot spot cells in time and space as identified using the Getis-Ord  statistic.
  
Technology used: Apache Spark, Hadoop Distributed File System (HDFS), Scala, sbt build tool, Amazon EC2
