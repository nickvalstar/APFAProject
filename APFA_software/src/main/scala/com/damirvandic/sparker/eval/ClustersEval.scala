package com.damirvandic.sparker.eval

import com.damirvandic.sparker.core.{Clusters, ProductDesc}
import com.damirvandic.sparker.data.DataSet
import org.slf4j.LoggerFactory

import scala.collection.Map

trait ClustersEval {
  def evaluate(computedClusters: Clusters, correctClusters: Clusters): Results
}

class ClustersEvalImpl extends ClustersEval {
  val logger = LoggerFactory.getLogger(getClass)

  override def evaluate(computedClusters: Clusters, correctClusters: Clusters): Results = {
    //    val size = computedClusters.clusterSizes.sum
    //    DurationHelper.timeMillis(dur => logger.debug(s"eval of $size entities took $dur ms")) {
    //    }
    eval(computedClusters, correctClusters)
  }

  private def eval(computedClusters: Clusters, correctClusters: Clusters): Results = {
    if (computedClusters.descriptions != correctClusters.descriptions) {
      val a_s1 = computedClusters.clusterCount
      val a_s2 = computedClusters.descriptions.size
      val b_s1 = correctClusters.clusterCount
      val b_s2 = correctClusters.descriptions.size
      throw new IllegalArgumentException(s"Algorithm has not included all product descriptions in the result! ($a_s1 vs $b_s1 cluster count en $a_s2 vs $b_s2 count)")
    }


    val correctClusterMap = DataSet.createIndex(correctClusters.asSet)
    val sizes = correctClusters.clusterSizes

    val positives = sizes.map(pairsCount).sum.toInt
    val negatives = (pairsCount(sizes.sum) - positives).toInt

    val (tp, fp) = computeStats(computedClusters, correctClusterMap)
    val fn = positives - tp
    val tn = negatives - fp
    ResultsImpl(tp, fp, tn, fn)
  }

  private def computeStats(computedClusters: Clusters, correctClusterMap: Map[ProductDesc, Set[ProductDesc]]): (Int, Int) = {
    var TP, FP = 0
    computedClusters.foreach(cluster => {
      val clusterSeq = cluster.toIndexedSeq

      val n = clusterSeq.size
      var i = 0
      while (i < n) {
        var j = i + 1
        val p1 = clusterSeq(i)
        while (j < n) {
          val p2: ProductDesc = clusterSeq(j)
          val isCorrect = correctClusterMap(p1) eq correctClusterMap(p2)
          if (isCorrect) TP += 1
          else FP += 1
          j += 1
        }
        i += 1
      }
    })
    (TP, FP)
  }

  private def pairsCount(n: Int) = {

    def factorial(n: BigInt): BigInt = {
      def loop(n: BigInt, acc: BigInt): BigInt = if (n <= 0) acc else loop(n - 1, acc * n)
      loop(n, 1)
    }

    assert(n > 0)
    factorial(n) / (factorial(2) * factorial(n - 2))
  }
}