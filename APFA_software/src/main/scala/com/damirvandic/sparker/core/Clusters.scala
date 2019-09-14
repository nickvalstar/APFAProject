package com.damirvandic.sparker.core

import org.slf4j.LoggerFactory

import scala.collection.convert.decorateAll._

trait Clusters {
  def toArray = asSet.toArray

  def asSet: Set[Set[ProductDesc]]

  def asJavaSet: java.util.Set[java.util.Set[ProductDesc]] = asSet.map(_.asJava).asJava

  def foreach(f: (Set[ProductDesc]) => Unit): Unit

  //  private lazy val _javaDescriptions: java.util.Set[ProductDesc] = descriptions.asJava

  def clusterCount: Int

  def descriptions: Set[ProductDesc]

  def javaDescriptions: java.util.Set[ProductDesc] = descriptions.asJava

  def get(i: Int): Set[ProductDesc]

  def javaGet(i: Int): java.util.Set[ProductDesc] = get(i).asJava

  def clusterSizes: Seq[Int]

}

object NoClusters extends Clusters {
  override def asSet: Set[Set[ProductDesc]] = Set.empty

  override def clusterCount: Int = 0

  override def clusterSizes: Seq[Int] = Seq.empty

  override def get(i: Int): Set[ProductDesc] = throw new UnsupportedOperationException

  override def descriptions: Set[ProductDesc] = Set.empty

  override def foreach(f: (Set[ProductDesc]) => Unit): Unit = ()
}

case class ClustersImpl(clusters: Set[Set[ProductDesc]]) extends Clusters {
  private lazy val clustersList = clusters.toArray

  override lazy val descriptions: Set[ProductDesc] = clusters.flatten

  override lazy val clusterSizes: Seq[Int] = clusters.toSeq.map(_.size)

  override val clusterCount: Int = clusters.size

  override def get(i: Int): Set[ProductDesc] = clustersList(i)

  override def foreach(f: (Set[ProductDesc]) => Unit): Unit = clusters.foreach(f)

  override def asSet: Set[Set[ProductDesc]] = clusters
}

case class ClustersBuilder() {
  val log = LoggerFactory.getLogger(getClass)

  val mappings: scala.collection.mutable.Map[ProductDesc, Any] = scala.collection.mutable.Map.empty
  val clusterMappings: scala.collection.mutable.Map[Any, scala.collection.mutable.Set[ProductDesc]] = scala.collection.mutable.Map.empty

  def assignToCluster(product: ProductDesc, identifier: Any) = {
    if (mappings.contains(product)) {
      log.warn(s"Product '$product' is already assigned to cluster '${mappings(product)}' (now overriden by '$identifier'")
      //      throw new IllegalArgumentException(s"Product '$product' is already assigned to cluster with id '$identifier'")
    }
    mappings.put(product, identifier)
    val cluster = if (!clusterMappings.contains(identifier)) {
      val c = scala.collection.mutable.Set.empty[ProductDesc]
      clusterMappings.put(identifier, c)
      c
    } else {
      clusterMappings(identifier)
    }
    cluster.add(product)
  }

  def build(): Clusters = new ClustersImpl(clusterMappings.values.map(_.toSet).toSet)

  def fromSets(clusters: Set[Set[ProductDesc]]): Clusters = ClustersImpl(clusters)

  def fromSets(clusters: java.util.Set[java.util.Set[ProductDesc]]): Clusters = ClustersImpl(clusters.asScala.map(_.asScala.toSet).toSet)
}
