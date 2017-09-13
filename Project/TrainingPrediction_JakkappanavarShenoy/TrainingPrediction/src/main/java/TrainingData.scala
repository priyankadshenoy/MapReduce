import java.io.Serializable
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * @author Jakkappanavar Shenoy
  *
  *         Main class for loading the labeled data to generate model for prediction.
  */
object TrainingData extends Serializable{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TrainingData").setMaster("yarn");
    val sc = new SparkContext(conf)

    val inputLines = sc.textFile(args(0) + "/training");

    val preprocessJob = new Preprocessor();

    val data = preprocessJob.run(inputLines);

    // Load and parse the data file.
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    var categoricalFeaturesInfo =
      Map[Int, Int](1 -> 13, 0 -> 6, 9 -> 38, 10 -> 10, 11 -> 10, 12 -> 10)

//    for(key <- 19 to 951) {
//      categoricalFeaturesInfo += (key -> 2)
//    }

    val numTrees = 75 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 15
    val maxBins = 40

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, args(0)+"/target/tmp/myRandomForestClassificationModel")

    sc.stop()
  }
}