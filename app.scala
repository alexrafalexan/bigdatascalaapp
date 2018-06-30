
import magellan.{Point, Polygon, PolyLine}
import magellan.{Point, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine
// import magellan.coord.NAD83
import org.apache.spark.sql.magellan._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import scala.reflect.runtime.universe._
import org.apache.spark.sql.Row
import magellan.Polygon
import java.io.IOException
import java.io.FileNotFoundException
import org.apache.hadoop.mapred.InvalidInputException
import java.lang.ArrayIndexOutOfBoundsException
import java.lang.IllegalArgumentException
import org.apache.spark.sql.AnalysisException
import java.lang.NumberFormatException
import scala.util.control.Breaks._
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}



val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

// equation for csv to df
def loadfile(file: String): DataFrame = {
  val spark = SparkSession.builder().getOrCreate()
  var check = false
  var dfa: DataFrame = null
  // Create first dataframe
  while (check == false) {
    println(s"Select the $file file:")
    val a = repl.in.readLine("Write full path:")
    println(s"$a")
    try {
      val customSchema = StructType(Array(
        StructField("tid", IntegerType, true),
        StructField("date", TimestampType, true),
        StructField("long", DoubleType, true),
        StructField("lat", DoubleType, true)))
      var dfa: DataFrame = spark.read.option("header", "true").schema(customSchema).option("inferSchema", "true").csv(s"$a")
      println(s"$file file Loaded..")
      dfa.show(5)
      //  dfa.schema()
      check = true
      return dfa
    }
    catch {
      case ex: AnalysisException => {
        println("File not found. Please enter correct FIRST file path")
        check = false
      }
      case ex: IllegalArgumentException => {
        println("Enter correct file type")
        check = false
      }
    }
  }
  return dfa
}

//equation for trajectories dataframe
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
case class TrRecord(tid: String, timestamp: String, point: Point)
def loadfiletrajectories (): DataFrame = {
  val trloadfile: DataFrame = null
  var check = true
  while (check == true){
    println("Select the A file")
    val a = repl.in.readLine("Write full path:")
    println(s"$a")
    println("Select how the file split")
    val splitarg = repl.in.readLine("Write the split argument:")
    println(s"$splitarg")
    try {
      val trloadfile = sc.textFile(s"$a").map{line =>
        val parts = line.split(s"$splitarg")
        val tid = parts(0)
        val timestamp = parts(1)
        val point = Point(parts(2).toDouble, parts(3).toDouble)
        TrRecord(tid,timestamp,point)
      }.toDF()
      check = false
      trloadfile.show()
      return trloadfile
    }
    catch {
      case ex: InvalidInputException =>{
        println("File does not exist")
        check = true
      }
      case ex: IllegalArgumentException => {
        println("Insert a path format")
        check = true
      }
      case ex: Exception =>{
        println("Delimeter error")
        check = true
      }
    }
  }
  return trloadfile
}

//load polygon files
def loadfolderpolygon (): DataFrame ={
  var polygonsDF:DataFrame = null
  var check = true
  while (check == true){
    println("Select the Polygon file")
    val b = repl.in.readLine("Write full path:")
    println(s"$b")
    try {
      val polygonsDF = sqlContext.read.format("magellan").
        load(s"$b").
        select($"polygon", $"metadata").
        cache().toDF()
      check = false
      polygonsDF.show()
      return polygonsDF
    } catch {
      case ex: IOException => {
        println("Please enter correct Second file path")
      }
      case ex: IllegalArgumentException => {
        println("Insert a path format")
        check = true
      }
    }
  }
  return polygonsDF
}

//repartition
def repartition(dfname: DataFrame, file: String): DataFrame = {
  var dftemprepart: DataFrame = null
  var part: Int = dfname.rdd.partitions.size
  var check = true
  while(check == true){
    println(s"$file DataFrame has $part partitions")
    println("Do you want to repartition?(y/n)")
    var a:String = repl.in.readLine("Select answer: ")
    if (a.equals("y")) {
      var checkpartition = true
      while(checkpartition == true){
        println("Select repartition number")
        try {
          var part = (repl.in.readLine("Number: ")).toInt
          var dftemprepart = dfname.repartition(part)
          println(s"You select $part partitions for $file")
          checkpartition = false
          return dftemprepart
        } catch {
          case e: IllegalArgumentException => {
            println("Enter correct partition number")
            checkpartition = true
          }
        }
      }
      return dftemprepart
      check = false
    }else if (a.equals("n")) {
      println(s"You will keep $part partitions for $file")
      var check = false
      dftemprepart = dfname
      return dftemprepart
    } else {
      println("Select y or n")
      var check = true
    }
  }
  return dftemprepart
}

//time range join

import sqlContext.implicits._
case class Time(time_start: String, time_end: String)
def timerangejoin() : DataFrame = {
  val tstart = repl.in.readLine("Write Start_Timestamp (yyyy-MM-dd HH:mm:ss) :")
  val tend = repl.in.readLine("Write End_Timestamp (yyyy-MM-dd HH:mm:ss) :")
  val timeDS = Seq(Time(s"$tstart",s"$tend")).toDF
  val timetimestamp = timeDS.select (unix_timestamp ($"time_start","yyyy-MM-dd HH:mm:ss")
    .cast(TimestampType).as("time_start"),
    unix_timestamp ($"time_end","yyyy-MM-dd HH:mm:ss")
      .cast(TimestampType).as("time_end"))
  timetimestamp.show()
  return timetimestamp
}

//join trajectories with polygons
def trajecjoinpolygon(jfile1: DataFrame, jfile2: DataFrame): DataFrame ={
  var jDF = jfile1.join(jfile2).
    where($"point".within($"polygon")).
    select($"tid", $"timestamp",$"metadata").
    withColumnRenamed("v", "neighborhood").drop("k")
  var checkjoinoption: Boolean = true
  while (checkjoinoption == true) {
    println("Select 0, 1, 2 or 3")
    var jointypeselection: Int = -1
    try{
      jointypeselection = (repl.in.readLine("(0-->sort by id join, 1--> sort by time join, 2 --> sort by id and time join, 3 --> time-range join):")).toInt
    }
    catch{
      case e: NumberFormatException => {
        println("Insert integer 0, 1, 2 or 3")
      }
    }
    if (jointypeselection == 0) {
      import sqlContext.implicits._
      jDF = jDF.orderBy($"tid".asc)
      return jDF
      checkjoinoption = false
    }
    else if (jointypeselection == 1) {
      import sqlContext.implicits._
      jDF = jDF.orderBy($"timestamp".asc)
      return jDF
      checkjoinoption = false
    }
    else if (jointypeselection == 2) {
      import sqlContext.implicits._
      jDF = jDF.orderBy($"tid".asc,$"timestamp".asc)
      return jDF
      checkjoinoption = false
    }
    else if (jointypeselection == 3) {
      import sqlContext.implicits._
      val timetable = timerangejoin()
      timetable.show()
      checkjoinoption = false
      jDF = timetable.join(jDF, timetable.col("time_start") <= jDF.col("timestamp") && timetable.col("time_end") >= jDF.col("timestamp"),"inner").select(jDF.col("tid"), jDF.col("timestamp"),jDF.col("metadata"))
      jDF.show()
      return jDF
    }
    else {
      println("Select 0, 1, 2, or 3")
    }
  }
  return jDF
}

// main
def main(): List[DataFrame] = {
  var file1partitioned: DataFrame = null
  var file2partitioned: DataFrame = null
  var joinedDF: DataFrame = null
  var check2: Boolean = true
  var first: String = "First"
  var second: String = "Second"
  var fileTypeSelection: Int = -1
  while (check2 == true) {
    println("Select the type of files you want to join")
    try{
      fileTypeSelection = (repl.in.readLine("(0-->trajectories with trajectories, 1-->trajectories with polygon file):")).toInt
    }
    catch{
      case e: NumberFormatException => {
        println("Insert integer 0 or 1")
      }
    }
    if (fileTypeSelection == 0) {
      val file1: DataFrame = loadfile(s"$first")
      val file2: DataFrame = loadfile(s"$second")
      check2 = false
      var file1partitioned: DataFrame = repartition(file1,s"$first")
      var file2partitioned: DataFrame = repartition(file2,s"$second")
      var joinedDF: DataFrame = trajecjoinpolygon(file1partitioned,file2partitioned)
      return List(file1partitioned,file2partitioned,joinedDF)
    }
    else if (fileTypeSelection == 1) {
      val file1: DataFrame = loadfiletrajectories()
      val file2: DataFrame = loadfolderpolygon()
      check2 = false
      var file1partitioned: DataFrame = repartition(file1,s"$first")
      var file2partitioned: DataFrame = repartition(file2,s"$second")
      var joinedDF: DataFrame = trajecjoinpolygon(file1partitioned,file2partitioned)

      return List(file1partitioned,file2partitioned,joinedDF)
    }
    else {
      println("Select 0 or 1")
      check2 == true
    }
  }
  return List(file1partitioned,file2partitioned,joinedDF)
}

//call main
val listDF = main()
listDF(2).show()
