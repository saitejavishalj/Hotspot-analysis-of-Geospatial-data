# Large Scale Geo-Spatial Data Analysis using SparkSQL

## CSE 512 - Distributed and Parallel Database Systems

## Project description

The project was aimed to setup a spark cluster with HDFS and run SparkSQL queries (geo-spatial) on the it.

* Native spark cluster was used as cluster manager.
* Hadoop Distributed File System (HDFS) was used as distributed storage system.
* The setup was done using Amazon EC2 virtual machines as nodes.
* Spatial queries such as range query, range join query, distance query, distance join query, hot zone analysis and hot cell analysis were executed.
  - Spatial queries were executed by implementing user defined functions such as ST_contains and ST_within in Scala.
  - ST_contains takes a point and a rectangle and returns a boolean indicating whether the point is inside the rectangle.
  - ST_within takes two points and a distance and returns a boolean indication whether the distance between the points is not more than the distance provided.
  
Technology used: Apache Spark, Hadoop Distributed File System (HDFS), Scala, sbt build tool, Amazon EC2

## Team members
1. Bhavani Balasubramanyam
2. Jagdeesh Basavaraju
3. Sahan Vishwas
4. Suraj Somachand Kattige
