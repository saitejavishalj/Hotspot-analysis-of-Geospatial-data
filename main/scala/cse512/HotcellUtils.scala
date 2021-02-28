package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def CalculateG (minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Double, maxZ: Double, numCells: Double, meanXj: Double, SXj: Double, curX: Int, curY: Int, curZ: Int, curNXj: Int): Double =
  {
    var Wij = 27
    if (curX == minX || curX == maxX) {
      Wij = 18
      if (curY == minY || curY == maxY) {
        Wij = 12
        if (curZ == minZ || curZ == maxZ) {
          Wij = 8
        }
      }
    }

    val numerator = curNXj - (meanXj*Wij)
    val in1 = numCells*Wij
    val in2 = Wij*Wij
    val in3 = in1 - in2
    val in4 = math.sqrt(in3/(numCells - 1))
    val denominator = SXj*in4
    val G = numerator/denominator
    return G
  }
}
