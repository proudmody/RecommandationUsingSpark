
import org.apache.spark.{SparkConf, SparkContext}
import test.recommadation.Initial

object DataSummary {
  def main(args: Array[String]): Unit = {
    val (sc,sqlContext) =Initial.Initial()
    //
    val UserArtCntFilter = sqlContext.read.parquet("D:\\data\\als\\als\\uacData")
    val d = UserArtCntFilter.selectExpr("user as USER1","*").drop("user")
    val c = UserArtCntFilter.selectExpr("user as USER2","artist","cnt")
    val join = c.join(d,d("USER1")===c("USER2"))
    println(join)
    sqlContext.udf.register("Comb", (arg1: String, arg2: String) => arg1+"-"+arg2)
    //count（收听次数） 的分布
    val maxcnt=sqlContext.sql("select comb(cnt,count) as value from cc")
    maxcnt.write.text("D:\\data\\als\\als\\countcout")
  }
}

