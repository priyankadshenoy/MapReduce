import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.immutable.List

/**
  * Created by ps on 3/13/17.
  */

object PageRankScala {
  def main(args: Array[String]) {
    try {

      var time1= java.lang.System.currentTimeMillis();

      val sparkCont = new SparkContext( new SparkConf().setAppName("PageRank").setMaster("local"))

      //Parsing file through PreProcessing java job
      val inputValues = sparkCont.textFile(args(0), sparkCont.defaultParallelism)
        .map(line => PageRankPreProcessing.readXML(line))         // iterating over every line of input
        .filter(line => !line.contains("IncorrectValue"))         // removes incorrect values due to tilde or other ill formatted pages
        .map(line => line.split("prishen"))                       // separator between page and link names
        .map(line => if (line.length == 1) {
          (line(0), List())                                       // if outlinks do not exist assign empty list
        } else {
          (line(0), line(1).split("~").toList)                    // splitting node
        })


      inputValues.persist()                                       // persisting values for faster access

      var uniqueNodeWithLinks = inputValues.values                // finding dangling nodes
        .flatMap { node => node }                                 // combining list of outlinks
        .keyBy(node => node)
        .map(line => (line._1, List[String]())).union(inputValues).reduceByKey((value1, value2) => value1.++(value2))   // dangling node with outlink is empty list


      val noOfNodes = uniqueNodeWithLinks.count()

      val loop: Int = 10                                          // defining constants
      val initialPageRank: Double = 1.0 / noOfNodes
      val alpha: Double = 0.15
      var uniqueNodeWithPageRank = uniqueNodeWithLinks.keys
        .map(line => (line, initialPageRank))                     // assigning initial page rank to all nodes


      for (i <- 1 to loop) {
        try {
          var danglingValue = sparkCont.accumulator(0.0)        // accumulator to store dangling factor
          var pageRankSetValues = uniqueNodeWithLinks.join(uniqueNodeWithPageRank)        // join does full join by key thereby removing unnecessary data
            .values
            .flatMap {
              case (links, pageRank) =>
                val size = links.size
                if (size == 0) {
                  danglingValue += pageRank
                  List()                                        // update dangling factor and assign empty list for dangling node
                } else {
                  links.map(url => (url, pageRank / size))      // update page rank values if not dangling node
                }
            }.reduceByKey(_ + _)                                // Combines page rank contribution of outlinks to node

          pageRankSetValues.first()

          val danglingValueAcc: Double = danglingValue.value


          uniqueNodeWithPageRank = uniqueNodeWithPageRank.subtractByKey(pageRankSetValues)  // finding node with no inlinks
            .map(rec => (rec._1 ,0.0)).union(pageRankSetValues)                             // setting value of page rank for nodes without inlinks to 0
                                                                                            // union with page rank with links
            .mapValues
            [Double](pageRankAcc => alpha * initialPageRank +
              (1 - alpha) * (danglingValueAcc / noOfNodes + pageRankAcc))                   // updating values of page rank for all outlinks

        }

        catch {
          case e: Exception => e
        }
      }

      var sortedVal = uniqueNodeWithPageRank.takeOrdered(100)(Ordering[Double]            // sorting and picking top 100
        .reverse.on { line => line._2 })

      sparkCont.parallelize(sortedVal)
        .saveAsTextFile(args(1))

    }
    catch {
      case e: Exception => e
    }
  }
}

