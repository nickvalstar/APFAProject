package com.damirvandic.sparker.algorithm

import java.util.UUID

import ch.usi.inf.sape.hac.agglomeration.{CompleteLinkage, AgglomerationMethod}
import com.damirvandic.sparker.core.{Clusters, ClustersBuilder, ProductDesc}
import org.apache.hadoop.conf.Configuration

import scala.collection.convert.decorateAll._
import scala.collection.mutable

trait Algorithm {
  /**
   * Full, human-readable name of the algorithm
   */
  def name: String

  def computeClusters(conf: scala.collection.Map[String, Any], data: scala.collection.Set[ProductDesc]): Clusters

  /**
   * Computes the clusters given a set of product descriptions. This method will be called on the same Algorithm instance
   * for different bootstraps and parameter configurations. You can implement internal caching. However, make sure that
   * you do use too much memory. I suggest using a cache that is bounded (e.g., using the Google Guava Cache library).
   * @param conf the configuration of parameters to use
   * @param data the product descriptions
   * @return a Clusters object (you can create one using the ClustersBuilder class)
   */
  def computeClusters(conf: java.util.Map[String, Any], data: java.util.Set[ProductDesc]): Clusters = {
    computeClusters(conf.asScala, data.asScala)
  }

  val variableMap: mutable.Map[String, String] = mutable.Map.empty

  def registerVariable(key: String, value: String): Unit = {
    variableMap.put(key, value)
  }

  def clearAllVariables(): Unit = {
    variableMap.clear()
  }
}

abstract class JavaAlgorithm extends Algorithm {

  @throws[RuntimeException]
  override def computeClusters(conf: scala.collection.Map[String, Any], data: scala.collection.Set[ProductDesc]): Clusters = {
    computeClusters(conf.asJava, data.asJava)
  }

  /**
   * Computes the clusters given a set of product descriptions. This method will be called on the same Algorithm instance
   * for different bootstraps and parameter configurations. You can implement internal caching. However, make sure that
   * you do use too much memory. I suggest using a cache that is bounded (e.g., using the Google Guava Cache library).
   * @param conf the configuration of parameters to use
   * @param data the product descriptions
   * @return a Clusters object (you can create one using the ClustersBuilder class)
   */
  override def computeClusters(conf: java.util.Map[String, Any], data: java.util.Set[ProductDesc]): Clusters

  protected def hierarchicalClustering(items: java.util.List[ProductDesc], data: java.util.Map[(ProductDesc, ProductDesc), java.lang.Double], epsilon: Double): Clusters = {
    val dMap = data.asScala
    val itemsIndexed = items.asScala.toArray
    val clusters = Clustering.findClusters(itemsIndexed, dMap, epsilon)
    new ClustersBuilder().fromSets(clusters)
  }

  protected def hierarchicalClustering(items: java.util.List[ProductDesc],
                                       data: java.util.Map[(ProductDesc, ProductDesc), java.lang.Double],
                                       epsilon: Double,
                                       agglomerationMethod: AgglomerationMethod): Clusters = {
    val dMap = data.asScala
    val itemsIndexed = items.asScala.toArray
    val clusters = Clustering.findClusters(itemsIndexed, dMap, epsilon, agglomerationMethod)
    new ClustersBuilder().fromSets(clusters)
  }
}

trait AlgorithmFactory extends Serializable {
  def build(cachingPath: String, hadoopConfig: java.util.Map[String, String]): Algorithm
}
