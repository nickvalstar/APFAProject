package com.damirvandic.sparker

import com.damirvandic.sparker.algorithm.Algorithm
import com.damirvandic.sparker.core._
import com.damirvandic.sparker.data.{DataSet, Reader}
import com.damirvandic.sparker.eval.ClustersEvalImpl
import org.scalatest._

import scala.collection.convert.decorateAsJava._

class ClustersEvalSpec extends FlatSpec with ShouldMatchers {
  "The ClustersEval implementation" should "correctly compute the metrics (example 1)" in {
    val indicies = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 6)
    val correctClusters = createClusters("a,b|c,d|e,f", indicies)
    val computedClusters = createClusters("a,b,c|d|e,f", indicies)
    val eval = new ClustersEvalImpl()
    val m = eval.evaluate(computedClusters, correctClusters)
    m.precision should be(0.50)
    m.recall should be(2 / 3.0)
    m.specificity should be(0.8333 +- .001)
    m.accuracy should be(0.80)
    m.f1 should be(0.571428 +- .001)
  }

  it should "correctly compute the metrics (full data set)" in {
    val dataset = Reader.readDataSet("dataset-TVs", "./hdfs/TVs-all-merged.json")
    val algorithm = new PerfectAlgorithm(dataset.clusters)
    val inputData = dataset.descriptions
    val computedClusters = algorithm.computeClusters(Map.empty[String, Any], inputData)
    val eval = new ClustersEvalImpl()
    val m = eval.evaluate(computedClusters, dataset.clusters)
    m.precision should be(1.0)
    m.recall should be(1.0)
    m.specificity should be(1.0)
    m.accuracy should be(1.0)
    m.f1 should be(1.0)
  }

  it should "correctly compute the metrics (full data set), with fromSets" in {
    val dataset = Reader.readDataSet("dataset-TVs", "./hdfs/TVs-all-merged.json")
    val algorithm = new PerfectAlgorithmFromSets(dataset.clusters)
    val inputData = dataset.descriptions
    val computedClusters = algorithm.computeClusters(Map.empty[String, Any], inputData)
    val eval = new ClustersEvalImpl()
    val m = eval.evaluate(computedClusters, dataset.clusters)
    m.precision should be(1.0)
    m.recall should be(1.0)
    m.specificity should be(1.0)
    m.accuracy should be(1.0)
    m.f1 should be(1.0)
  }

  private val DUMMY_MAP = Map("test1" -> "testtting", "test2" -> "testtting", "test3" -> "testtting").asJava

  private def createClusters(input: String, indicies: Map[String, Int]): Clusters = {
    val index = scala.collection.mutable.Map.empty[String, ProductDesc]
    val ret = scala.collection.mutable.Set.empty[Set[ProductDesc]]
    var i = 0
    for (clusterStr <- input.split("\\|")) {
      val s = scala.collection.mutable.Set.empty[ProductDesc]
      for (node <- clusterStr.split(",")) {
        val p =
          if (index.contains(node)) {
            index.get(node).get
          } else {
            val temp = new ProductDesc(indicies(node), "title_" + node, "model_" + node, "url_" + node, DUMMY_MAP)
            index.put(node, temp)
            i += 1
            temp
          }
        s.add(p)
      }
      ret.add(s.toSet)
    }
    ClustersBuilder().fromSets(ret.toSet)
  }

  private class PerfectAlgorithm(correctClusters: Clusters) extends Algorithm {
    /**
     * Full, human-readable name of the algorithm
     */
    override def name: String = "Perfect Algorithm"

    override def computeClusters(conf: collection.Map[String, Any], data: collection.Set[ProductDesc]): Clusters = {
      val ii = DataSet.createIndex(correctClusters.asSet).mapValues(_.hashCode().toString)
      val clusterBuilder = ClustersBuilder()
      for (p <- data) {
        val clusterID = ii.get(p).toString
        clusterBuilder.assignToCluster(p, clusterID)
      }
      clusterBuilder.build()
    }
  }

  private class PerfectAlgorithmFromSets(correctClusters: Clusters) extends Algorithm {
    /**
     * Full, human-readable name of the algorithm
     */
    override def name: String = "Perfect Algorithm"

    override def computeClusters(conf: collection.Map[String, Any], data: collection.Set[ProductDesc]): Clusters = {
      val ii = DataSet.createIndex(correctClusters.asSet).mapValues(_.hashCode().toString)
      val clusterBuilder = ClustersBuilder()

      val groups = data.map(p => (ii.get(p).toString, p)).groupBy(_._1).mapValues(_.map(_._2).toSet)
      clusterBuilder.fromSets(groups.values.toSet)
    }
  }

}
