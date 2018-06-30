
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


val spark = SparkSession.builder().getOrCreate()

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
      var dfa: DataFrame = spark.read.option("header", "false").option("inferSchema", "true").csv(s"$a")
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
      return dfname
    } else {
      println("Select y or n")
      var check = true
    }
  }
  return dftemprepart
}

// main
def main(): Unit = {
  println("Hello from main of class")
  var check2: Boolean = true
  var first: String = "First"
  var second: String = "Second"
  while (check2 == true) {
    println(s"Select the type of files you want to join")
    var fileTypeSelection: Int = (repl.in.readLine("(0-->trajectories with trajectories, 1-->trajectories with polygon file):")).toInt
    if (fileTypeSelection == 0) {
      val file1: DataFrame = loadfile(s"$first")
      val file2: DataFrame = loadfile(s"$second")
      check2 = false
      val file1partioned: DataFrame = repartition(file1,s"$first")
      val file2partioned: DataFrame = repartition(file2,s"$second")
    }
    else if (fileTypeSelection == 1) {
      val file1: DataFrame = loadfiletrajectories()
      val file2: DataFrame = loadfolderpolygon()
      check2 = false
      val file1partioned: DataFrame = repartition(file1,s"$first")
      val file2partioned: DataFrame = repartition(file2,s"$second")
    }
    else {
      println("Select 0 or 1")
      check2 == true
    }
  }
}

//call main
val aaa = main()
