import org.apache.spark.{SparkConf, SparkContext}
import test.recommadation.Initial

object DataMapStars {
  def markStar(count:Int):Int={
    //次数->星级
    if ((count==1) && (count==2)) 1
    else if (count>=3 && count <=7) 2
    else  if (count>=8 && count <= 15) 3
    else if (count>=16 &&count<=30) 4
    else  5
  }
  def main(args: Array[String]): Unit = {
    val (sc,sqlContext) =Initial.Initial()
    import sqlContext.implicits._

    val UserArtCntFilter = sqlContext.read.parquet("D:\\data\\als\\als\\uacData")
    val UserArtStars = UserArtCntFilter.map{
      uac=>
        (uac.getString(0),uac.getString(1),markStar(uac.getString(2).toInt))
    }.toDF("user","artist","stars")
    UserArtStars.write.parquet("D:\\data\\als\\als\\uaStars")
  }
}

