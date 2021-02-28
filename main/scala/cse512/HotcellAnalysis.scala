package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    val sumXj = pickupInfo.count()

    // YOU NEED TO CHANGE THIS PART
    val countDf = pickupInfo.groupBy("x","y","z").count().withColumnRenamed("count", "Xj")
    // countDf.show()

    // val squares = countDf.selectExpr("(Xj * Xj) as sqr").select(sum("sqr"))
    val squares = countDf.selectExpr("(Xj * Xj) as sqr")
    // squares.show()

    val sumofsq = squares.agg(sum("sqr")).first.getLong(0)
    val meanXj = sumXj/numCells
    val in1 = sumofsq.toDouble/numCells
    val in2 = meanXj*meanXj
    val SXj = math.sqrt(in1 - in2)

    // printf("Sum Xj =  %d\n", sumXj)
    // printf("Total cells =  %f\n", numCells)
    // printf("Mean Xj =  %f\n", meanXj)
    // printf("sum of squares: %d\n", sumofsq)
    // printf("SD: %f\n", SXj)
    
    val withneighborcountDf = countDf.as("count1").join(countDf.as("count2"),
    (col("count1.x") === col("count2.x") || col("count1.x") === col("count2.x") - 1 || col("count1.x") === col("count2.x") + 1) && (col("count1.y") === col("count2.y") || col("count1.y") === col("count2.y") - 1 || col("count1.y") === col("count2.y") + 1) && (col("count1.z") === col("count2.z") || col("count1.z") === col("count2.z") - 1 || col("count1.z") === col("count2.z") + 1),"inner").select(col("count1.x").as("x"), col("count1.y").as("y"), col("count1.z").as("z"), col("count2.Xj").as("NXj") )


    // withneighborcountDf.show(50)

    val sumNXj = withneighborcountDf.groupBy("x","y","z").agg(sum("NXj")).withColumnRenamed("sum(NXj)", "sumNXj")
    sumNXj.createOrReplaceTempView("sumNXj")
    // sumNXj.show(50)

    spark.udf.register("CalculateG",(curX: Int, curY: Int, curZ: Int, curNXj: Int)=>((
      HotcellUtils.CalculateG(minX, maxX, minY, maxY, minZ, maxZ, numCells, meanXj, SXj, curX, curY, curZ, curNXj)
    )))
    val withGscore = spark.sql("select *, CalculateG(a.x,a.y,a.z,a.sumNXj) as Gscore from sumNXj as a")
    val descCells = withGscore.orderBy(col("Gscore").desc).limit(50).select(col("x"),col("y"),col("z"))
    descCells.createOrReplaceTempView("descCells")
    descCells.show(50)

    return descCells
  }
}
