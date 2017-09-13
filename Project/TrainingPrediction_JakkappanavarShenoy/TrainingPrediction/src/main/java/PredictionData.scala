import java.io.Serializable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.model.RandomForestModel


/**
  * @author Jakkappanavar Shenoy
  *
  *         Main class for predicting the unlabelled data using the model generated
  */
object PredictionData extends Serializable{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PredictionData").setMaster("yarn");
    val sc = new SparkContext(conf)

    val inputLines = sc.textFile(args(0) + "/prediction");

    val preprocessJob = new Preprocessor();

    val data = preprocessJob.runPrediction(inputLines);

    val model = RandomForestModel.load(sc, args(0) + "/target/tmp/myRandomForestClassificationModel")

    // Evaluate model on test instances and compute test error
    val labelAndPreds = data.map { point =>
      val prediction = model.predict(point.features)
      ("S" + point.label.toInt + "," + prediction.toInt)
    }

    labelAndPreds.repartition(1).saveAsTextFile(args(1) + "/prediction")

    sc.stop()
  }
}