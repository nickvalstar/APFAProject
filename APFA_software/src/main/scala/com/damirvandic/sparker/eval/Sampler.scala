package com.damirvandic.sparker.eval

import com.damirvandic.sparker.core.{Clusters, ClustersBuilder, ProductDesc}
import com.damirvandic.sparker.data.DataSet
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.collection.convert.decorateAsJava._

case class Sample(train: Set[ProductDesc], test: Set[ProductDesc], trainClusters: Clusters, testClusters: Clusters) {
  def jTrain: java.util.Set[ProductDesc] = train.asJava
  def jTest: java.util.Set[ProductDesc] = test.asJava
}

case class RDDSample(train: RDD[ProductDesc], test: RDD[ProductDesc], trainClusters: Clusters, testClusters: Clusters)

trait Sampler {
  def sample(dataSet: DataSet): Sample
}

class SamplerImpl(seed: Long) extends Sampler {
  val r = new Random(seed)

  def sample(dataSet: DataSet): Sample = {
    val clusters = dataSet.clusters.toArray
    val n = clusters.size
    val trainingClusters = Seq.fill(n)(clusters(r.nextInt(n))).toSet
    val testClusters = clusters.filterNot(trainingClusters.contains).toSet
    val training = trainingClusters.flatten
    val test = testClusters.flatten
    val clusterBuilder = ClustersBuilder()
    Sample(training, test, clusterBuilder.fromSets(trainingClusters), clusterBuilder.fromSets(testClusters))
  }
}
