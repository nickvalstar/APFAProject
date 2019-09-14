package com.damirvandic.sparker

import com.damirvandic.sparker.algorithm._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.util.Try

class ClusteringSpec extends FlatSpec with MustMatchers with BeforeAndAfterAll {
  val data1 = Seq(
    ("A" -> "B", .9),
    ("A" -> "C", .95),
    ("A" -> "D", .89),
    ("A" -> "E", .91),
    ("A" -> "F", .95),
    ("B" -> "C", .02),
    ("B" -> "D", .4),
    ("B" -> "E", .4),
    ("B" -> "F", .7),
    ("C" -> "D", .4),
    ("C" -> "E", .4),
    ("C" -> "F", .7),
    ("D" -> "E", .03),
    ("D" -> "F", .1),
    ("E" -> "F", .1)
  )
  val items = Vector("A", "B", "C", "D", "E", "F")
  var sc: SparkContext = _

  "The Clustering Module" must "correctly find clusters" in {
    val data = sc.parallelize(data1)
    val dMap = RDDConverter.dissMapConvertPair(data)
    val clusters = Clustering.findClusters(items, dMap, 0.3)
    clusters must have size 3
    clusters contains Set("A") mustBe true
    clusters contains Set("B", "C") mustBe true
    clusters contains Set("D", "E", "F") mustBe true
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
}
