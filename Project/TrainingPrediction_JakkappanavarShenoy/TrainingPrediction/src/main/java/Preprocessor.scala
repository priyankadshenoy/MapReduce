import java.io.Serializable
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.{ArrayBuffer}

/**
  *   @author Jakkappanavar Shenoy
  *
  *           Preprocessor to clean the data and provide data in the format helpful for training
  */
class Preprocessor extends Serializable {

  /**
    * Processes the input bz2 file and returns an RDD of LabeledPoint for training
    */
  def run(inputLines: RDD[String]) : RDD[LabeledPoint] = {

    createLabeledPoints(inputLines);

  }

  /**
    * Processes the input bz2 file and returns an RDD of LabeledPoint for prediction
    */
  def runPrediction(inputLines: RDD[String]) : RDD[LabeledPoint] = {

    createLabeledPointsForPrediction(inputLines);

  }

  /**
    * Creates an RDD of Labelled points from the RDD of lines for training
    */
  def createLabeledPoints(inputLines: RDD[String]) : RDD[LabeledPoint] = {
    val inputLinesFiltered = inputLines
                                .filter(filterOnQuestionMark);
    val inputLinesMapped = inputLinesFiltered
                                .map(createLabeledPoint);

    inputLinesMapped;
  }

  /**
    * Creates an RDD of Labelled points from the RDD of lines for prediction
    */
  def createLabeledPointsForPrediction(inputLines: RDD[String]) : RDD[LabeledPoint] = {
    val inputLinesFiltered = inputLines
                                .filter(filterOnQuestionMark);
    val inputLinesMapped = inputLinesFiltered
                                .map(createLabeledPointForPrediction);

    inputLinesMapped;
  }

  /**
    *
    * @return Boolean
    *         Filters the header and all lines having Year as "?"
    */
  def filterOnQuestionMark = {
    (line: String) =>
      val lineParts = line.split(",");
      val year = lineParts(AppConstants.YEAR_COL_INDEX);
      val month = lineParts(AppConstants.MONTH_COL_INDEX);
      val day = lineParts(AppConstants.DAY_COL_INDEX);

      (year != "?"
        && year != "YEAR"
        && month != "?"
        && day != "?")
  }

  /**
    * Takes a line as a String and return a LabeledPoint object for training
    */
  def createLabeledPoint = {
    (line: String) =>

      var lineParts = line.split(",");

      val label = getLabel(lineParts(AppConstants.TARGET_BIRD_COL_INDEX));

      val features = getSelectedFeatures(lineParts);

      LabeledPoint(label, Vectors.dense(features));

  }

  /**
    *
    * @param lineParts : Array[String]
    * @return Array[Double]
    *
    *         Provides the identified features in the returned array.
    */

  def getSelectedFeatures(lineParts: Array[String]) : Array[Double] = {
    var features = ArrayBuffer[Double]();

    // Count type index 0
    features += getCountTypeVal(lineParts(AppConstants.COUNT_TYPE_COL_INDEX)).toDouble

    // Month index 1
    val month = lineParts(AppConstants.MONTH_COL_INDEX)
    if(month == "X")
      features += 0.0
    else
      features += month.toDouble

    // Number of Observers index 2
    val numObservers = lineParts(AppConstants.NUMBER_OF_OBSERVERS_COL_INDEX)
    if(numObservers == "X"
        || numObservers == "?")
      features += 1.0
    else
      features += numObservers.toDouble

    // Effort Hrs index 3
    val effortHrs = lineParts(AppConstants.EFFORT_HRS_COL_INDEX)
    if(effortHrs == "X"
      || effortHrs == "?")
      features += 0.5
    else
      features += effortHrs.toDouble

    // Effort Dist index 4
    val effortDist = lineParts(AppConstants.EFFORT_DIST_COL_INDEX)
    if(effortDist == "X"
      || effortDist == "?")
      features += 1.0
    else
      features += effortDist.toDouble

    // POP00_SQMI index 5
    val pop00 = lineParts(AppConstants.POP00_SQMI)
    if(pop00 == "X"
      || pop00 == "?")
      features += 0.0
    else
      features += pop00.toDouble

    // HOUSING_DENSITY index 6
    val housingDensity = lineParts(AppConstants.HOUSING_DENSITY_COL_INDEX)
    if(housingDensity == "X"
      || housingDensity == "?")
      features += 0.0
    else
      features += housingDensity.toDouble

    // ELEV_GT index 7
    val elevGT = lineParts(AppConstants.ELEV_GT_COL_INDEX)
    if(elevGT == "X"
      || elevGT == "?")
      features += 1.0
    else
      features += elevGT.toDouble

    // ELEV_NED index 8
    val elevNED = lineParts(AppConstants.ELEV_NED_COL_INDEX)
    if(elevNED == "X"
      || elevNED == "?")
      features += 0.0
    else
      features += elevNED.toDouble

    // BCR index 9
    val bcr = lineParts(AppConstants.BCR_COL_INDEX)
    if(bcr == "X"
      || bcr == "?")
      features += 1.0
    else
      features += bcr.toDouble

    // CAUS_TEMP_AVG index 10
    val causTempAvg = lineParts(AppConstants.CAUS_TEMP_AVG_COL_INDEX)
    if(causTempAvg == "X"
      || causTempAvg == "?")
      features += 1.0
    else
      features += causTempAvg.toDouble

    // CAUS_PREC index 11
    val causPrec = lineParts(AppConstants.CAUS_PREC_COL_INDEX)
    if(causPrec == "X"
      || causPrec == "?")
      features += 1.0
    else
      features += causPrec.toDouble

    // CAUS_SNOW index 12
    val causSnow = lineParts(AppConstants.CAUS_SNOW_COL_INDEX)
    if(causSnow == "X"
      || causSnow == "?")
      features += 1.0
    else
      features += causSnow.toDouble

    // DIST_FROM_FLOWING_FRESH index 13
    val distFromFlowingFresh = lineParts(AppConstants.DIST_FROM_FLOWING_FRESH_COL_INDEX)
    if(distFromFlowingFresh == "X"
      || distFromFlowingFresh == "?")
      features += 1.0
    else
      features += distFromFlowingFresh.toDouble

    // DIST_IN_FLOWING_FRESH index 14
    val distInFlowingFresh = lineParts(AppConstants.DIST_IN_FLOWING_FRESH_COL_INDEX)
    if(distInFlowingFresh == "X"
      || distInFlowingFresh == "?")
      features += 1.0
    else
      features += distInFlowingFresh.toDouble

    // DIST_FROM_STANDING_FRESH index 15
    val distFromStandingFresh = lineParts(AppConstants.DIST_FROM_STANDING_FRESH_COL_INDEX)
    if(distFromStandingFresh == "X"
      || distFromStandingFresh == "?")
      features += 1.0
    else
      features += distFromStandingFresh.toDouble

    // DIST_IN_STANDING_FRESH index 16
    val distInStandingFresh = lineParts(AppConstants.DIST_IN_STANDING_FRESH_COL_INDEX)
    if(distInStandingFresh == "X"
      || distInStandingFresh == "?")
      features += 1.0
    else
      features += distInStandingFresh.toDouble

    // DIST_FROM_WET_VEG_FRESH index 17
    val distFromWetVegFresh = lineParts(AppConstants.DIST_FROM_WET_VEG_FRESH_COL_INDEX)
    if(distFromWetVegFresh == "X"
      || distFromWetVegFresh == "?")
      features += 1.0
    else
      features += distFromWetVegFresh.toDouble

    // DIST_IN_WET_VEG_FRESH index 18
    val distInWetVegFresh = lineParts(AppConstants.DIST_IN_WET_VEG_FRESH_COL_INDEX)
    if(distInWetVegFresh == "X"
      || distInWetVegFresh == "?")
      features += 1.0
    else
      features += distInWetVegFresh.toDouble

    /**
      * Adding birds as feature
      */

    /*for(birdIndex <- AppConstants.FIRST_BIRD_COL_INDEX to AppConstants.LAST_BIRD_COL_INDEX) {
      if(birdIndex != AppConstants.TARGET_BIRD_COL_INDEX) {
        if(lineParts(birdIndex) == "?"
          || lineParts(birdIndex) == "X"
          || lineParts(birdIndex) == "0.0")
          features += 0.0
        else
          features += 1.0
      }
    }*/

    features.toArray
  }

  /**
    * Takes a line as a String and return a LabeledPoint object for prediction
    */
  def createLabeledPointForPrediction = {
    (line: String) =>

      val lineParts = line.split(",");

      val label = lineParts(AppConstants.SAMPLING_ID_COL_INDEX).substring(1).toDouble

      val features = getSelectedFeatures(lineParts);

      LabeledPoint(label, Vectors.dense(features));

  }

  /**
    *
    * @return Double
    *
    *         Used for label. Values are 1 or 0
    */
  def getLabel = {
    (labelVal: String) =>
      if(labelVal == "?"
        || labelVal == "0")
        0.0;
      else
        1.0;
  }

  /**
    *
    * @return Int
    *         Gives one of the count value according to reference document
    */
  def getCountTypeVal = {
    (countTypeVal: String) =>
      countTypeVal match {
        case "P21" => 1
        case "P22" => 2
        case "P34" => 2
        case "P23" => 3
        case "P35" => 3
        case "P20" => 4
        case _ => 5
      }
  }

}