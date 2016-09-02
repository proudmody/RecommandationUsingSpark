package test.recommadation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Initial {
  def Initial():(SparkContext,SQLContext)= {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample").
      setMaster("local[8]").
      set("spark.executor.memory", "12g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    (sc,sqlContext)
  }
}
// scalastyle:on println
