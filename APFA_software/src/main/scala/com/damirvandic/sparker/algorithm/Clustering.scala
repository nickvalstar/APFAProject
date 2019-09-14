package com.damirvandic.sparker.algorithm

import ch.usi.inf.sape.hac.HierarchicalAgglomerativeClusterer
import ch.usi.inf.sape.hac.agglomeration._
import ch.usi.inf.sape.hac.dendrogram.{Dendrogram, DendrogramBuilder}
import ch.usi.inf.sape.hac.experiment.{DissimilarityMeasure, Experiment, ListBasedExperiment, SparseDissimilarityMeasure}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.convert.decorateAll._
import scala.reflect.ClassTag

object RDDConverter {
  def dissMapConvertTriple[T: ClassTag](data: RDD[(T, T, java.lang.Double)]): Map[(T, T), java.lang.Double] = data.map(t => ((t._1, t._2), t._3.asInstanceOf[java.lang.Double])).collect().toMap

  def dissMapConvertPairJava[T: ClassTag](data: RDD[((T, T), java.lang.Double)]): Map[(T, T), java.lang.Double] = data.map(t => (t._1, t._2.asInstanceOf[java.lang.Double])).collect().toMap

  def dissMapConvertPair[T: ClassTag](data: RDD[((T, T), Double)]): Map[(T, T), java.lang.Double] = data.collect().map(t => (t._1, t._2.asInstanceOf[java.lang.Double])).toMap
}

object Clustering {

  def findClusters[T](items: IndexedSeq[T],
                      dMap: Map[(T, T), java.lang.Double],
                      epsilon: Double,
                      agglomerationMethod: AgglomerationMethod = new CompleteLinkage
                       ): Set[Set[T]] = {
    val experiment: Experiment[T] = new ListBasedExperiment(items.asJava)
    val dissimilarityMeasure: DissimilarityMeasure[T] = new SparseDissimilarityMeasure(dMap.toSeq.asJava)
    val dendrogramBuilder: DendrogramBuilder = new DendrogramBuilder(experiment.getNumberOfObservations)
    val clusterer = new HierarchicalAgglomerativeClusterer(experiment, dissimilarityMeasure, agglomerationMethod)
    clusterer.cluster(dendrogramBuilder)
    val dendrogram: Dendrogram = dendrogramBuilder.getDendrogram
    val clusters: Set[Set[T]] = getClusters(items, dendrogram.getClusters(epsilon))
    clusters
  }

  def findJavaClusters[T](items: java.util.List[T],
                      dMap: java.util.Map[(T, T), java.lang.Double],
                      epsilon: Double,
                      agglomerationMethod: AgglomerationMethod = new CompleteLinkage
                       ): java.util.Set[java.util.Set[T]] = {
    val ret = findClusters[T](items.asScala.toIndexedSeq, dMap.asScala.toMap, epsilon, agglomerationMethod).map(_.asJava).asJava
    ret
  }

  private def getClusters[T](items: IndexedSeq[T], clusters: java.util.Set[java.util.Set[Integer]]): Set[Set[T]] = {
    clusters.asScala.map(_.asScala.map(i => items(i)).toSet).toSet
  }
}
