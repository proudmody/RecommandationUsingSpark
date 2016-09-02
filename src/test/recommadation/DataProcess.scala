
import org.apache.spark.{SparkConf, SparkContext}
import test.recommadation.Initial
import scala.collection.mutable.Map

object DataProcess {
  def main(args: Array[String]): Unit = {
    val (sc,sqlContext) =Initial.Initial()
    import sqlContext.implicits._

    val alias = sc.textFile("D:\\data\\als\\artist_alias.txt").repartition(8)
    //过滤脏数据
    val aliasFilter = alias.map{
      d=>
        d.split("\t")
    }.filter{
      d=> if (d(0)=="") false
        else true
    }.map{
      d=>
        (d(0),d(1))
    }
    alias.cache()
    //artist info
    val artist = sc.textFile("D:\\data\\als\\artist_data.txt").repartition(8)
    val artistArray= artist.map{
      a=>a.split("\t")
    }.map{
      a=>a(0)
    }.collect()
    artist.cache()
    //映射艺术家的正确id
    val aliasArray = aliasFilter.collect()
    val bad2good = Map.empty[String,String]
    aliasArray.foreach{
     a=>
       bad2good(a._1)=a._2
    }
    val artistId = Map.empty[String,Int]
    artistArray.foreach{
      a=>
        artistId(a)=1
    }
    //处理数据
    //得到完整可用的 user-artist-count表
    val UserArtCnt = sc.textFile("D:\\data\\als\\user_artist_data.txt").repartition(8)
    UserArtCnt.cache()
    val errorcnt =sc.accumulator(0)
    val UserArtCntFilter = UserArtCnt.map{
      uac =>
        uac.split(" ")
    }.map{
      case Array(user,artist,cnt) =>
        val alias = bad2good.getOrElseUpdate(artist,"error")
        val error:String = "error"
        if (artistId.get(artist)==Some(1)) (user,artist,cnt)
        else if (alias!=null) (user,alias,cnt)
        else {
          (user, error,cnt)
        }
    }.filter(
      uac=>
        if (uac._2!="error") true else false
    ).toDF("user","artist","cnt")
    UserArtCntFilter.write.parquet("D:\\data\\als\\als\\uacData")
  }
}
