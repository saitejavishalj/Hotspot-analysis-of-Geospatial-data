package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    val ST_Contains = (queryRectangle:String, pointString:String) => {
      val rectPts = queryRectangle.split(",")
      var minrx:Float = rectPts(0).toFloat
      var maxrx:Float = rectPts(2).toFloat
      var minry:Float = rectPts(1).toFloat
      var maxry:Float = rectPts(3).toFloat
      if (minrx > maxrx) {
        var temp = minrx
        minrx = maxrx
        maxrx = temp
      }
      if (minry > maxry) {
        var temp = minry
        minry = maxry
        maxry = temp
      }
      val pts = pointString.split(",")
      var xp:Float = pts(0).toFloat
      var yp:Float = pts(1).toFloat
      if (xp <= maxrx && xp >= minrx && yp <= maxry && yp >= minry) {
        true
      } else {
        false
      }
    }
    spark.udf.register("ST_Contains",ST_Contains)

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    val ST_Contains = (queryRectangle:String, pointString:String) => {
      val rectPts = queryRectangle.split(",")
      var minrx:Float = rectPts(0).toFloat
      var maxrx:Float = rectPts(2).toFloat
      var minry:Float = rectPts(1).toFloat
      var maxry:Float = rectPts(3).toFloat
      if (minrx > maxrx) {
        var temp = minrx
        minrx = maxrx
        maxrx = temp
      }
      if (minry > maxry) {
        var temp = minry
        minry = maxry
        maxry = temp
      }
      val pts = pointString.split(",")
      var xp:Float = pts(0).toFloat
      var yp:Float = pts(1).toFloat
      if (xp <= maxrx && xp >= minrx && yp <= maxry && yp >= minry) {
        true
      } else {
        false
      }
    }
    spark.udf.register("ST_Contains",ST_Contains)

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    val ST_Within = (pointString1:String, pointString2:String, distance:Double) => {
      val pt1 = pointString1.split(",")
      val x1:Double = pt1(0).toDouble
      val y1:Double = pt1(1).toDouble
      val pt2 = pointString2.split(",")
      val x2:Double = pt2(0).toDouble
      val y2:Double = pt2(1).toDouble
      var xdiff:Double = x2-x1
      var ydiff:Double = y2-y1
      val ecd:Double = math.sqrt(math.pow(xdiff,2)+math.pow(ydiff,2))
      if (ecd <= distance)
        true
      else
        false
    }
    spark.udf.register("ST_Within",ST_Within)

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    val ST_Within = (pointString1:String, pointString2:String, distance:Double) => {
      val pt1 = pointString1.split(",")
      val x1:Double = pt1(0).toDouble
      val y1:Double = pt1(1).toDouble
      val pt2 = pointString2.split(",")
      val x2:Double = pt2(0).toDouble
      val y2:Double = pt2(1).toDouble
      var xdiff:Double = x2-x1
      var ydiff:Double = y2-y1
      val ecd:Double = math.sqrt(math.pow(xdiff,2)+math.pow(ydiff,2))
      if (ecd <= distance)
        true
      else
        false
    }
    spark.udf.register("ST_Within",ST_Within)
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
