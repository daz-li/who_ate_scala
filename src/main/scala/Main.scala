import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.Try

case class Query(ts: Long, rd: Int)

object QueryParser {
  val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromJson(line: String): Option[Query] = 
    Try(mapper.readValue(line, classOf[Query])).toOption
  
  def toJson(o: Any): String = mapper.writeValueAsString(o)
}

object Fn {
  def filter(tsbegin: Long, tsend: Long, q: Query): Boolean = 
    (q.ts >= tsbegin && q.ts < tsend) &&  q.rd == 1
}

object Main {
  val sc = new SparkContext(new SparkConf()) 

  def filter(tsbegin: Long, tsend: Long, q: Query): Boolean = 
    (q.ts >= tsbegin && q.ts < tsend) &&  q.rd == 1

  def main(args: Array[String]) {
    println("who.ate.main: " + args.toList)
    val input::output::tsbegin::tsend::flag::rest= args.toList

    val f = filter(tsbegin.toLong, tsend.toLong, _: Query)
    val g = (q: Query) => filter(tsbegin.toLong, tsend.toLong, q)
    val h = Fn.filter(tsbegin.toLong, tsend.toLong, _: Query)

    val i = flag match {
      case "fn_in_main_obj_1" => f
      case "fn_in_main_obj_2" => g
      case "fn_out_main_obj"  => h
    }

    val rdd = sc.textFile(input)
      .flatMap{QueryParser.fromJson}
      .filter(i)
      .map(QueryParser.toJson)
      .saveAsTextFile(output)

    println("who.ate.main done.")
  }
}
