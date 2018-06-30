
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
case class TaxiRecord(tid: String, timestamp: String, point: Point)
def loadfiletrajectories (): DataFrame = {
  val taxiloadfile: DataFrame = null
  var check = true
  while (check == true){
    println("Select the A file")
    val a = repl.in.readLine("Write full path:")
    println(s"$a")
    println("Select how the file split")
    val splitarg = repl.in.readLine("Write the split argument:")
    println(s"$splitarg")
    try {
      val taxiloadfile = sc.textFile(s"$a").map{line =>
        val parts = line.split(s"$splitarg")
        val tid = parts(0)
        val timestamp = parts(1)
        val point = Point(parts(2).toDouble, parts(3).toDouble)
        TaxiRecord(tid,timestamp,point)
      }.toDF()
      check = false
      taxiloadfile.show()
      return taxiloadfile
    }
    catch {
      case ex: InvalidInputException =>{
        println("DEN YPARXEI ARXEIO")
        check = true
      }
      case ex: IllegalArgumentException => {
        println("TO EISAGOMENO DN EINAI MORFIS PATH")
        check = true
      }
      case ex: Exception =>{
        println("LATHOS DELIMITER")
        check = true
      }
    }
  }
  return taxiloadfile
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
        println("TO EISAGOMENO DN EINAI MORFIS PATH")
        check = true
      }
    }
  }
  return polygonsDF
}


// load files
var check2: Boolean = true
while (check2 == true) {
  println(s"Select the type of files you want to join")
  var fileTypeSelection: Int = (repl.in.readLine("(0-->trajectories with trajectories, 1-->trajectories with polygon file):")).toInt
  if (fileTypeSelection == 0) {
    var first: String = "First"
    var second: String = "Second"
    val file1: DataFrame = loadfile(s"$first")
    val file2: DataFrame = loadfile(s"$second")
    check2 = false
  }
  else if (fileTypeSelection == 1) {
    val file1: DataFrame = loadfiletrajectories()
    val file2: DataFrame = loadfolderpolygon()
    check2 = false
  }
  else {
    println("Select 0 or 1")
    check2 == true
  }
}
