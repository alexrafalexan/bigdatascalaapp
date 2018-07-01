import magellan.{Point, Polygon, PolyLine}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine
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
import org.apache.spark.sql.functions.broadcast

//IMPORTANT
val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

//function Import File Routes to (LONG,LAT or LAT,long)
def importdata(file: String): DataFrame = {
  val spark = SparkSession.builder().getOrCreate()
  var check = false
  var dfa: DataFrame = null
  var long: String = null
  var lat: String = null
  // Create first dataframe
  while (check == false) {
    println(s"Select the $file file:")
    val a = repl.in.readLine("Write full path:")
    println(s"$a")
    println ("Select Schema of coordinate system")
    var checklonglat: Int = -1
    var loopcontition: Boolean = true
    while (loopcontition == true){
      try{
        checklonglat = (repl.in.readLine("Select 1 for ID,TIMESTAMP,LONG,LAT , Select 2 for ID,TIMESTAMP,LAT,LONG:")).toInt
      }
      catch{
        case e: NumberFormatException => {
          println("Select 1 for ID,TIMESTAMP,LONG,LAT , Select 2 for ID,TIMESTAMP,LAT,LONG:")
        }
      }
      if (checklonglat == 1){
        long = "LONG"
        lat = "LAT"
        loopcontition = false
      } else if (checklonglat == 2){
        long = "LAT"
        lat = "LONG"
        loopcontition = false
      } else {
        loopcontition = true
      }
    }
    try {
      val customSchema = StructType(Array(
        StructField("ID", IntegerType, true),
        StructField("TIMESTAMP", TimestampType, true),
        StructField(s"$long", DoubleType, true),
        StructField(s"$lat", DoubleType, true)))
      var dfa: DataFrame = spark.read.option("header", "true").schema(customSchema).option("inferSchema", "true").option("delimiter", "\t").csv(s"$a")
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
//function Import File Routes to Schema point
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
case class RouteRecord(ID: Int, TIMESTAMP: String, POINT: Point)
def importdatatopoint (file: String): DataFrame = {
  val routes: DataFrame = null
  var checkcondition = true
  while (checkcondition == true){
    println(s"Select $file")
    val typed = repl.in.readLine("Write full path:")
    println(s"$typed")
    println("Select how the file split")
    val sarg = repl.in.readLine("Write the split argument:")
    println(s"$sarg")
    try {
      val routes = sc.textFile(s"$typed").map{line =>
        val rows = line.split(s"$sarg")
        val id = rows(0).toInt
        val timstmp = rows(1)
        val point = Point(rows(3).toDouble, rows(2).toDouble)
        RouteRecord(id,timstmp,point)
      }.toDF()
      checkcondition = false
      routes.show()
      return routes
    }
    catch {
      case e: IllegalArgumentException => {
        println("Enter path format")
        checkcondition = true
      }
      case e: InvalidInputException =>{
        println("Enter existing file")
        checkcondition = true
      }
      case e: Exception =>{
        println("Delimeter error")
        checkcondition = true
      }
    }
  }
  return routes
}
//function Import polygon data , convert with magellan to Polygon Table
def importpolygondata (file: String): DataFrame ={
  var polDF:DataFrame = null
  var checkoption: Boolean = true
  while (checkoption == true){
    try {
      println(s"Select $file (Polygon folder)")
      val typed = repl.in.readLine("Folder path:")
      println(s"$typed")
      polDF = sqlContext.read.format("magellan").
        load(s"$typed").
        select($"polygon", $"metadata").
        cache().toDF()
      polDF.show(10,false)
      checkoption = false
    } catch {
      case ex: IllegalArgumentException => {
        println("Enter path folder format")
      }
      case ex: IOException => {
        println("Enter correct polygon folder path")
      }
    }
  }
  return polDF
}
//function to Repartition
def defpartition (df: List [DataFrame], opt: List[Any]): List [DataFrame] = {
  var dfname0: DataFrame = null
  var dfname1: DataFrame = null
  for (i <- 0 to 1){
    var part: Int = df(i).rdd.partitions.size
    var check = true
    while(check == true){
      var option: String = opt(i).toString
      println(s"$option DataFrame has $part partitions")
      println("Do you want to repartition?(y/n)")
      var a:String = repl.in.readLine("Select answer: ")
      if (a.equals("y")) {
        var checkpartition = true
        while(checkpartition == true){
          println("Select repartition number")
          try {
            part = (repl.in.readLine("Number: ")).toInt
            var dftemp:DataFrame = df(i).repartition(part)
            if (i == 0){
              dfname0 = dftemp
            }else{
              dfname1 = dftemp
            }
            println(s"You select $part partitions for $option")
            checkpartition = false
          } catch {
            case e: IllegalArgumentException => {
              println("Enter correct partition number")
            }
          }
        }
        check = false
      }else if (a.equals("n")) {
        println(s"You will keep $part partitions for $option")
        if (i == 0){
          dfname0 = df(i)
        }else{
          dfname1 = df(i)
        }
        check = false
      } else {
        println("Select y or n")
      }
    }
  }
  println(dfname0.rdd.partitions.size)
  println(dfname1.rdd.partitions.size)
  return List(dfname0,dfname1)
}
//function to Join Tables of Routes
def joinroutes(df: List[DataFrame]): DataFrame ={
  var finalDF: DataFrame = null
  var joincheck: Boolean = true
  while (joincheck == true) {
    println("Choose 1 or 2")
    var option: Int = -1
    try{
      option = (repl.in.readLine("(Select 1 for simple join, Select 2 for broadcast join):")).toInt
    }
    catch{
      case e: NumberFormatException => {
        println("(Select 0 for simple join, Select 1 for broadcast join):")
      }
    }
    if (option == 1) {
      finalDF = df(0).join(df(1), df(0).col("ID") === df(1).col("ID") && df(0).col("TIMESTAMP") === df(1).col("TIMESTAMP")).
        select(df(0).col("ID"),df(0).col("TIMESTAMP"),df(1).col("ID"),df(1).col("LONG"),df(1)("LAT"))
      joincheck = false
    }
    else if (option == 2) {
       finalDF = df(0).join(broadcast(df(1)),df(0).col("ID") === df(1).col("ID") && df(0).col("TIMESTAMP") === df(1).col("TIMESTAMP")).
        select(df(0).col("ID"),df(0).col("TIMESTAMP"),df(1).col("ID"),df(1).col("LONG"),df(1).col("LAT"))
      joincheck = false
    }
    else {
      println("(Select 0 for simple join, Select 1 for broadcast join):")
    }
  }
  return finalDF
}
//function to Join Table of Routes with Polygon Table
def joinpol(df: List[DataFrame]): DataFrame={
  var joinedDF = df(0).join(df(1)).
    where($"point".within($"polygon")).
    select($"ID", $"TIMESTAMP",$"metadata").
    withColumnRenamed("v", "neighborhood").drop("k")
  var checkjoinoption: Boolean = true
  while (checkjoinoption == true) {
    var jointypeselection: Int = -1
    try{
      jointypeselection = (repl.in.readLine("(Select 1 for join and sort by ID, Select 2 for join and sort by TIMESTAMP, Select 3 for join and sort by ID,TIMESTAMP, Select 4 time-Range join):")).toInt
    }
    catch{
      case e: NumberFormatException => {
        println("Select 1 for join and sort by ID, Select 2 for join and sort by TIMESTAMP, Select 3 for join and sort by ID,TIMESTAMP, Select 4 time-Range join):")
      }
    }
    if (jointypeselection == 1) {
      import sqlContext.implicits._
      joinedDF = joinedDF.orderBy($"tid".asc)
      checkjoinoption = false
    }
    else if (jointypeselection == 2) {
      import sqlContext.implicits._
      joinedDF = joinedDF.orderBy($"timestamp".asc)
      checkjoinoption = false
    }
    else if (jointypeselection == 3) {
      import sqlContext.implicits._
      joinedDF = joinedDF.orderBy($"tid".asc,$"timestamp".asc)
      checkjoinoption = false
    }
    else if (jointypeselection == 4) {
      import sqlContext.implicits._
      val timetable = timerangejoin()
      timetable.show()
      checkjoinoption = false
      joinedDF = timetable.join(joinedDF, timetable.col("time_start") <= joinedDF.col("TIMESTAMP")
        && timetable.col("time_end") >= joinedDF.col("TIMESTAMP"),"inner").
        select(joinedDF.col("ID"), joinedDF.col("timestamp"),joinedDF.col("metadata"))
      joinedDF.show()
    }
    else {
      println("Select 1 for join and sort by ID, Select 2 for join and sort by TIMESTAMP, Select 3 for join and sort by ID,TIMESTAMP, Select 4 time-Range join):")
    }
  }
  return joinedDF
}
//function create Table for time-rang join
import sqlContext.implicits._
case class Time(time_start: String, time_end: String)
def timerangejoin() : DataFrame = {
  val tstart = repl.in.readLine("Start_Time (yyyy/MM/dd HH:mm:ss) :")
  val tend = repl.in.readLine("End_Time (yyyy/MM/dd HH:mm:ss) :")
  val timeDS = Seq(Time(s"$tstart",s"$tend")).toDF
  val timetimestamp = timeDS.select (unix_timestamp ($"time_start","yyyy/MM/dd HH:mm:ss")
    .cast(TimestampType).as("time_start"),
    unix_timestamp ($"time_end","yyyy/MM/dd HH:mm:ss")
      .cast(TimestampType).as("time_end"))
  timetimestamp.show()
  return timetimestamp
}

//START
//First Function / Select what you want to do
def first(): List [Any] ={
  var first: String = "File A"
  var second: String = "File B"
  var chooseoption: Int = 0
  var check: Boolean = true
  println("The first file to join must contain routes defined by longtitude and latitude.")
  println("Select the type of the second file:")
  while (check == true) {
    try{
      chooseoption = (repl.in.readLine("(Select 1 for routes or 2 for shapefiles):")).toInt
    }
    catch{
      case e: NumberFormatException => {
        println("(Select 1 for routes or 2 for shapefiles):")
      }
    }
    if (chooseoption == 1) {
      check = false
    }
    else if (chooseoption == 2) {
      check = false
    }
    else {
      check == true
    }
  }
  return List(first,second,chooseoption)
}
//Secont Function / Take List from First function and call other functions
def second(opt: List[Any]): List[DataFrame] = {
  var opt0: String = opt(0).toString
  var opt1: String = opt(1).toString
  var opt2: Int = opt(2).toString.toInt
  var adf: DataFrame = null
  var bdf: DataFrame = null
  if (opt2 == 1) {
    adf = importdata(s"$opt0")
    bdf = importdata(s"$opt1")
  }
  else if (opt2 == 2) {
    adf = importdatatopoint(s"$opt0")
    bdf = importpolygondata(s"$opt1")
  }
  return List(adf,bdf)
}
// Main function
def main(): DataFrame = {
  val mfirst = first()
  val msecond = second(mfirst)
  val condition: Int  = mfirst(2).toString.toInt
  val partitions = defpartition(msecond,mfirst)
  var join: DataFrame = null
  if (condition==1){
    join = joinroutes(partitions)
  }
  else{
    join = joinpol(partitions)
  }
  return join
}

//CALL MAIN
val run = main()
//SHOW RESULT
run.show(5)
