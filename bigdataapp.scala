import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine
import java.io.IOException
import java.io.FileNotFoundException
power // Enable power user mode
var check = false
while(check == false){
println("Select the FIRST file:")
val a = repl.in.readLine("Write full path:")
println("Select the SECOND file:")
val b = repl.in.readLine("Write full path:")
println(s"$b")
try {
 val  dfa = spark.read.option("header","true").option("inferSchema","true").csv(s"$a")
 println("file 1 ok")
 dfa.show(5)
 check = true
}
catch {
case ex: FileNotFoundException => {
  println("File not found. Please enter correct FIRST file path")
  check = false
}
case ex: Exception => {
  println("File not found. Please enter correct FIRST file path")
  check = false
}
}
try {
 val  dfb = spark.read.option("header","true").option("inferSchema","true").csv(s"$b")
 println("file 2 ok")
  dfb.show(5)
 check = true
}
catch {
 case ex2: FileNotFoundException => {
   println("File not found. Please enter correct SECOND file path")
   check = false
 }
 case ex2: Exception => {
   println("File not found. Please enter correct SECOND file path")
   check = false
 }
}
}
