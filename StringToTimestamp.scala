import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row
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




val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._


val tstart = repl.in.readLine("Write Start_Timestamp (yyyy/MM/dd HH:mm:ss) :")
val tend = repl.in.readLine("Write End_Timestamp (yyyy/MM/dd HH:mm:ss) :")


case class Time(time_start: String, time_end: String)

val timeDS = Seq(Time(s"$tstart",s"$tend")).toDF

val timetimestamp = timeDS.select (unix_timestamp ($"time_start","yyy/MM/dd HH:mm:ss")
                    .cast(TimestampType).as("time_start"),
                    unix_timestamp ($"time_end","yyy/MM/dd HH:mm:ss")
                    .cast(TimestampType).as("time_end"))

timetimestamp.show()

var dfa: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("/usr/lib/spark/scala_files/taxi_data/taxi_123.csv")

dfa.show()

timetimestamp.join(dfa, timetimestamp.col("time_start") <= dfa.col("DATE") && timetimestamp.col("time_end") >= dfa.col("DATE"),"inner").select(dfa.col("NUM"), dfa.col("DATE")).show()
