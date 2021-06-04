# Large Scale Geo-Spatial Data Analysis using Apache Spark and SparkSQL

## Distributed and Parallel Database Systems

## Project description:

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

**Background:**
  The use of spatial statistics to discover statistically significant clusters or outliers in geographic data is a well-known analytic technique used by GIS experts.
When making decisions, spatial statistics are essential. For example, if we are 95% certain that the levels observed in this location surpass a regulation standard, we must respond accordingly. The Getis-Ord statistic is a widely used statistic for identifying statistically significant clusters (also referred to as Hot Spot Analysis). It provides a z-score and p-values, allowing users to see where characteristics with high or low values are spatially grouped. 
  It's worth noting that this statistic can be utilized in both spatial and spatio-temporal domains; however, we'll be focusing on the spatio-temporal Getis-Ord statistic in this competition. We currently live in a world where observational data is being collected at an ever-increasing rate, further complicating the difficulty of finding hot areas. Many firms and organizations, for example, track their mobile assets on a regular basis. This data is frequently quite important to businesses, both in terms of real-time analytics (e.g., geofencing) and batch analysis after the fact.
  Distributed processing approaches are necessary to manage these massive quantities of observational data (e.g., in the billions). Interest in the Apache Spark framework has exploded across government, industry, and academia over the last few years.Spark is a powerful software platform for constructing scalable distributed algorithms utilizing functional programming techniques and languages. It can be easily implemented on commodity hardware clusters. 
