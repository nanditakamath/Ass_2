package cse511

import org.apache.spark.sql.SparkSession
import scala.math._

object SpatialQuery extends App {

  // User-defined function for ST_Contains
  def stContains(queryRectangle: String, pointString: String): Boolean = {
    val point = pointString.split(",")
    val px = point(0).toDouble
    val py = point(1).toDouble

    val rect = queryRectangle.split(",")
    val x1 = rect(0).toDouble
    val y1 = rect(1).toDouble
    val x2 = rect(2).toDouble
    val y2 = rect(3).toDouble

    val minX = math.min(x1, x2)
    val maxX = math.max(x1, x2)
    val minY = math.min(y1, y2)
    val maxY = math.max(y1, y2)

    px >= minX && px <= maxX && py >= minY && py <= maxY
  }

  // User-defined function for ST_Within
  def stWithin(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val point1 = pointString1.split(",")
    val x1 = point1(0).toDouble
    val y1 = point1(1).toDouble

    val point2 = pointString2.split(",")
    val x2 = point2(0).toDouble
    val y2 = point2(1).toDouble

    val euclideanDistance = sqrt(pow(x2 - x1, 2) + pow(y2 - y1, 2))

    euclideanDistance <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf.createOrReplaceTempView("point")

    // Registering the ST_Contains UDF
    spark.udf.register("ST_Contains", stContains _)

    val resultDf = spark.sql("select * from point where ST_Contains('" + arg2 + "', point._c0)")
    resultDf.show()

    resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2)
    rectangleDf.createOrReplaceTempView("rectangle")

    // Registering the ST_Contains UDF
    spark.udf.register("ST_Contains", stContains _)

    val resultDf = spark.sql("select * from rectangle, point where ST_Contains(rectangle._c0, point._c0)")
    resultDf.show()

    resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf.createOrReplaceTempView("point")

    // Registering the ST_Within UDF
    spark.udf.register("ST_Within", stWithin _)

    val resultDf = spark.sql("select * from point where ST_Within(point._c0, '" + arg2 + "', " + arg3 + ")")
    resultDf.show()

    resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1)
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2)
    pointDf2.createOrReplaceTempView("point2")

    // Registering the ST_Within UDF
    spark.udf.register("ST_Within", stWithin _)

    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")")
    resultDf.show()

    resultDf.count()
  }
}
