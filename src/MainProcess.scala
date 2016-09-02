
import test.recommadation._


object MainProcess {

  def main(args: Array[String]): Unit = {
    //处理数据集
    DataProcess.main(null)
    //查看数据概要
    DataSummary.main(null)
    //根据数据分布，标记收听次数为1-5星
    DataMapStars.main(null)
    //训练
    TrainingWithAls.main(null)
    //测试训练模型
    TestModel.main(null)
  }
}

// scalastyle:on println
