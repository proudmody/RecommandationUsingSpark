package test.recommadation

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

import scala.collection.mutable.Map

object TestModel {
  def evl2Int(x: String): Int = {
    if (x == "null" || x==null) 0
    else
      try {
        x.toInt
      } catch {
        case ex: Exception => 0
      }
  }
  def main(args: Array[String]): Unit = {
    val (sc,sqlContext) =Initial.Initial()
    val sameModel = MatrixFactorizationModel.load(sc, "D:\\data\\als\\als\\CFModel")
    val artist = sc.textFile("D:\\data\\als\\artist_data.txt")
    val artistArray= artist.map{
      a=>a.split("\t")
    }.map{
      a=> if (a.length==2) (a(0),a(1))
      else ("null","null")
    }.collect()
    val artistInfo = Map.empty[Int,String]
    artistArray.foreach{
      a=>
        artistInfo(evl2Int(a._1))=a._2
    }
    val res1 = sameModel.recommendProducts(2205913,20)
    val res=res1
      res.foreach{
      rate=>
        println(artistInfo(rate.product)+"\t \t \t "+rate)
    }
  }
}
// scalastyle:on println
