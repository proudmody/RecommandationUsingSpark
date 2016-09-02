package test.recommadation

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import scala.collection.mutable.ListBuffer

object TrainingWithAls {
  def main(args: Array[String]): Unit = {
    val (sc,sqlContext) =Initial.Initial()

    val UserArtStars = sqlContext.read.parquet("D:\\data\\als\\als\\uaStars")
    val ratings = UserArtStars.map{
      d=>
      Rating(d.getString(0).toInt, d.getString(1).toInt, d.getInt(2).toDouble-3.0)
    }
    //随机拆分训练集和测试集
    val splits = ratings.randomSplit(Array(0.8, 0.2 ),11L)
    val (train, cval) = (splits(0), splits(1))
    train.cache()
    cval.cache()
    val ranks = new ListBuffer[Int]()
    //迭代rank
    for(i<- (4 to 4)){
      ranks.append(i * 10)
    }
    val res =new ListBuffer[(Int,Double)]()

    ranks.foreach{
      r=>
        val rank = r
        //可以调整的参数
        val numIterations = 20
        val regParam = 0.165
        val model = ALS.train(train, rank, numIterations, regParam)
        val usersProducts = cval.map { case Rating(user, product, rate) =>
          (user, product)
        }
        val predictions =
          model.predict(usersProducts).map { case Rating(user, product, rate) =>
            ((user, product), rate)
          }
        val ratesAndPreds = cval.map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }.join(predictions)
        val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
          val err = (r1 - r2)
          err * err
        }.mean()

        res.append((rank,MSE))
        model.save(sc, "D:\\data\\als\\als\\CFModel")
    }
    res.foreach(println)
  }
}
