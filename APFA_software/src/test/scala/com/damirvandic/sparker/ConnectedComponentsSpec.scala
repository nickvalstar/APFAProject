package com.damirvandic.sparker

import com.damirvandic.sparker.algorithm.{BlockingFunction, ConnectedComponents, ConnectedComponentsImpl}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.util.{Random, Try}

class ConnectedComponentsSpec extends FlatSpec with MustMatchers with BeforeAndAfterAll {
  var sc: SparkContext = _
  val cc: ConnectedComponents = new ConnectedComponentsImpl

  "The Distribute Connected Components Module" must "correctly find connected components in a simple graph" in {
    val data = sc.parallelize(Seq("A" -> "B", "B" -> "C", "D" -> "E"))
    val components = cc.findComponents(data, BlockingFunction.empty[String]).collect().groupBy(_._1).mapValues(_.map(_._2))
    val clusters = components.values.toSet.map((arr: Array[String]) => arr.toSet)
    clusters must have size 2
    clusters contains Set("A", "B", "C") mustBe true
    clusters contains Set("D", "E") mustBe true
  }

  it must "correctly find connected components in a simple graph (non-spark)" in {
    val data = Seq(
      "A" -> "A", "B" -> "B", "C" -> "C", "D" -> "D", "E" -> "E",
      "A" -> "B", "B" -> "C", "D" -> "E", "F" -> "F")
    val components = cc.findComponents(data, BlockingFunction.empty[String]).groupBy(_._1).mapValues(_.map(_._2))
    val clusters = components.values.toSet
    clusters must have size 3
    clusters contains Set("A", "B", "C") mustBe true
    clusters contains Set("D", "E") mustBe true
    clusters contains Set("F") mustBe true
  }

  it must "correctly find connected components in a complex graph" in {
    val edges = Seq(
      "A" -> "B",
      "B" -> "C",
      "B" -> "D",
      "B" -> "E",
      "E" -> "F",
      "G" -> "H",
      "G" -> "I",
      "H" -> "I",
      "H" -> "G", //duplicate
      "I" -> "G", //duplicate
      "I" -> "H", //duplicate
      "J" -> "K",
      "L" -> "K",
      "J" -> "M",
      "N" -> "M"
    )
    val data = sc.parallelize(edges)
    val components = cc.findComponents(data, BlockingFunction.empty[String]).collect().groupBy(_._1).mapValues(_.map(_._2))
    val clusters = components.values.toSet.map((arr: Array[String]) => arr.toSet)
    clusters must have size 3
    clusters contains Set("A", "B", "C", "D", "E", "F") mustBe true
    clusters contains Set("G", "H", "I") mustBe true
    clusters contains Set("J", "K", "L", "M", "N") mustBe true
  }

  it must "correctly find connected components in a complex graph (non-spark)" in {
    val data = Seq(
      "A" -> "B",
      "B" -> "C",
      "B" -> "D",
      "B" -> "E",
      "E" -> "F",
      "G" -> "H",
      "G" -> "I",
      "H" -> "I",
      "H" -> "G", //duplicate
      "I" -> "G", //duplicate
      "I" -> "H", //duplicate
      "J" -> "K",
      "L" -> "K",
      "J" -> "M",
      "N" -> "M"
    )
    val components = cc.findComponents(data, new BlockingFunction[String] {
      override def isBlocking(a: String, b: String): Boolean = (a == "N" && b == "J") || (a == "J" && b == "N")
    }).groupBy(_._1).mapValues(_.map(_._2))
    val clusters = components.values.toSeq.sortBy(_.size)
    clusters must have size 4
    clusters contains Set("G", "H", "I") mustBe true
    clusters contains Set("J", "K", "L", "M", "N") mustBe false
    clusters contains Set("A", "B", "C", "D", "E", "F") mustBe true
    clusters contains Set("J", "K", "L", "M") mustBe true
    clusters contains Set("N") mustBe true
  }

  it must "correctly find connected components when the result is 1 large cluster" in {
    val edges = Seq(
      "A" -> "B",
      "B" -> "C",
      "B" -> "D",
      "B" -> "E",
      "E" -> "F",

      "F" -> "G", // connecting edge

      "G" -> "H",
      "G" -> "I",
      "H" -> "I",

      "I" -> "J", // connecting edge


      "J" -> "K",
      "L" -> "K",
      "J" -> "M",
      "N" -> "M"
    )
    val data = sc.parallelize(edges)
    val cc: ConnectedComponents = new ConnectedComponentsImpl
    val components = cc.findComponents(data, BlockingFunction.empty[String]).collect().groupBy(_._1).mapValues(_.map(_._2))
    val clusters = components.values.toSet.map((arr: Array[String]) => arr.toSet)
    clusters must have size 1
    clusters contains Set("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N") mustBe true
  }

  it must "correctly find connected components when the result is 1 large cluster (non-spark)" in {
    val data = Seq(
      "A" -> "B",
      "B" -> "C",
      "B" -> "D",
      "B" -> "E",
      "E" -> "F",

      "F" -> "G", // connecting edge

      "G" -> "H",
      "G" -> "I",
      "H" -> "I",

      "I" -> "J", // connecting edge


      "J" -> "K",
      "L" -> "K",
      "J" -> "M",
      "N" -> "M"
    )
    val components = cc.findComponents(data, BlockingFunction.empty[String]).groupBy(_._1).mapValues(_.map(_._2))
    val clusters = components.values.toSet
    clusters must have size 1
    clusters contains Set("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N") mustBe true
  }

  it must "handle large data sets" in {
    val nClusters = 1629
    val minClusterSize = 4
    val maxClusterSize = 4
    val (clusters, pairs) = generateRandomGraph(nClusters, minClusterSize, maxClusterSize)

    val data = sc.parallelize(pairs.toSeq)
    val components = cc.findComponents(data, BlockingFunction.empty[String]).collect()

    val clustersFound = components.groupBy(_._1).mapValues(_.map(_._2)).values.toSet.map((arr: Array[String]) => arr.toSet)

    clustersFound must have size clusters.size
    clusters.subsetOf(clustersFound) mustBe true
  }

  it must "handle large data sets (non-spark)" in {
    val nClusters = 1629
    val minClusterSize = 4
    val maxClusterSize = 4
    val (clusters, pairs) = generateRandomGraph(nClusters, minClusterSize, maxClusterSize)

    val data = pairs.toSeq
    val components = cc.findComponents(data, BlockingFunction.empty[String])

    val clustersFound = components.groupBy(_._1).mapValues(_.map(_._2)).values.toSet

    clustersFound must have size clusters.size
    clusters.subsetOf(clustersFound) mustBe true
  }

  override protected def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("Sparker").setMaster("local[8]").set("spark.shuffle.spill", "false").set("spark.executor.memory", "1g")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    Try(if (sc != null) sc.stop())
    super.afterAll()
  }

  private def generateRandomGraph(nClusters: Int, minClusterSize: Int, maxClusterSize: Int): (Set[Set[String]], Seq[(String, String)]) = {
    val clusters = scala.collection.mutable.Set.empty[Set[String]]
    val pairs = scala.collection.mutable.ArrayBuffer.empty[(String, String)]

    val r = new Random(1234)
    var i = 0
    def randomCluster() = Seq.fill(r.nextInt(maxClusterSize - minClusterSize + 1) + minClusterSize)({
      val ret = i
      i += 1
      ret.toString
    }).toSet

    // generate random clusters
    for (i <- 0 until nClusters) {
      clusters.add(randomCluster())
    }

    // generate pairs from these clusters
    for (cluster <- clusters) {
      val clusterSeq = cluster.toIndexedSeq
      val n = clusterSeq.size
      for (i <- 0 until n) {
        for (j <- i until n) {
          pairs.append((clusterSeq(i), clusterSeq(j)))
          pairs.append((clusterSeq(i), clusterSeq(i)))
          pairs.append((clusterSeq(j), clusterSeq(j)))
        }
      }
    }

    (clusters.toSet, pairs.toSeq)
  }
}
