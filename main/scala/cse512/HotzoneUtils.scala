package cse512

object HotzoneUtils {

  // def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    // return true // YOU NEED TO CHANGE THIS PART
  // }

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rectPts = queryRectangle.split(",")
    var minrx:Double = rectPts(0).toDouble
    var maxrx:Double = rectPts(2).toDouble
    var minry:Double = rectPts(1).toDouble
    var maxry:Double = rectPts(3).toDouble
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
    var xp:Double = pts(0).toDouble
    var yp:Double = pts(1).toDouble
    if (xp <= maxrx && xp >= minrx && yp <= maxry && yp >= minry) {
      true
    } else {
      false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
