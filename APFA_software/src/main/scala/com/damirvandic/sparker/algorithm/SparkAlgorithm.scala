package com.damirvandic.sparker.algorithm

import java.util.UUID

import ch.usi.inf.sape.hac.agglomeration.AgglomerationMethod
import com.damirvandic.sparker.core.{Clusters, ClustersBuilder, ProductDesc}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD

import scala.collection.convert.decorateAll._
import scala.collection.{Map, mutable}

case class ProductClusterTuple(clusterID: Any, p: ProductDesc)

trait SparkAlgorithm {

  /**
   * Full, human-readable name of the algorithm
   */
  def name: String

  /**
   * Computes the clusters given a set of product descriptions. This method will be called on the same Algorithm instance
   * for different bootstraps and parameter configurations. You can implement internal caching. However, make sure that
   * you do use too much memory. I suggest using a cache that is bounded (e.g., using the Google Guava Cache library).
   * @param conf the configuration of parameters to use
   * @param data the product descriptions
   * @return an RDD of tuples (clusterID, product)
   */
  def computeClusters(conf: scala.collection.Map[String, Any], data: RDD[ProductDesc]): RDD[(String, ProductDesc)]

  def computeClusters(conf: java.util.Map[String, Any], data: JavaRDD[ProductDesc]): JavaPairRDD[String, ProductDesc] = {
    val ret = computeClusters(conf.asScala, data).toJavaRDD()
    new JavaPairRDD(ret)
  }

  def findComponents(data: RDD[(ProductDesc, ProductDesc)]): RDD[(String, ProductDesc)] = {
    new ConnectedComponentsImpl().findComponents(data, ProductDescBlockingFunction)
  }

  def findConnectedComponents(data: JavaPairRDD[ProductDesc, ProductDesc]): JavaPairRDD[String, ProductDesc] = {
    new JavaPairRDD(findComponents(data))
  }

  protected def hierarchicalClustering(items: java.util.List[ProductDesc],
                                       dMap: java.util.Map[(ProductDesc, ProductDesc), java.lang.Double],
                                       epsilon: Double): Clusters = {
    val itemsIndexed = items.asScala.toArray
    val clusters = Clustering.findClusters(itemsIndexed, dMap.asScala, epsilon)
    new ClustersBuilder().fromSets(clusters)
  }

  protected def hierarchicalClustering(items: java.util.List[ProductDesc],
                                       dMap: java.util.Map[(ProductDesc, ProductDesc), java.lang.Double],
                                       epsilon: Double,
                                       agglomerationMethod: AgglomerationMethod): Clusters = {
    val itemsIndexed = items.asScala.toArray
    val clusters = Clustering.findClusters(itemsIndexed, dMap.asScala, epsilon, agglomerationMethod)
    new ClustersBuilder().fromSets(clusters)
  }

  val variableMap: mutable.Map[String, String] = mutable.Map.empty

  def registerVariable(key: String, value: String): Unit = {
    variableMap.put(key, value)
  }

  def clearAllVariables(): Unit = {
    variableMap.clear()
  }
}

abstract class JavaSparkAlgorithm extends Serializable with SparkAlgorithm {

  override def computeClusters(conf: scala.collection.Map[String, Any], data: RDD[ProductDesc]): RDD[(String, ProductDesc)] = {
    computeClusters(conf.asJava, data.toJavaRDD())
  }

  protected def hierarchicalClustering(items: java.util.List[ProductDesc],
                                       data: JavaPairRDD[(ProductDesc, ProductDesc), java.lang.Double],
                                       epsilon: Double): JavaPairRDD[String, ProductDesc] = {
    val dMap: Map[(ProductDesc, ProductDesc), java.lang.Double] = RDDConverter.dissMapConvertPairJava(data.rdd)
    val itemsIndexed = items.asScala.toArray
    val clusters = Clustering.findClusters(itemsIndexed, dMap, epsilon)
    val expanded = clusters.toSeq.flatMap(c => {
      val clusterID = UUID.randomUUID().toString
      c.map(p => (clusterID, p))
    })
    val ret = data.sparkContext.parallelize(expanded).toJavaRDD()
    new JavaPairRDD[String, ProductDesc](ret)
  }

  protected def hierarchicalClustering(items: java.util.List[ProductDesc],
                                       data: JavaPairRDD[(ProductDesc, ProductDesc), java.lang.Double],
                                       epsilon: Double,
                                       agglomerationMethod: AgglomerationMethod): JavaPairRDD[String, ProductDesc] = {
    val dMap: Map[(ProductDesc, ProductDesc), java.lang.Double] = RDDConverter.dissMapConvertPairJava(data.rdd)
    val itemsIndexed = items.asScala.toArray
    val clusters = Clustering.findClusters(itemsIndexed, dMap, epsilon, agglomerationMethod)
    val expanded = clusters.toSeq.flatMap(c => {
      val clusterID = UUID.randomUUID().toString
      c.map(p => (clusterID, p))
    })
    val ret = data.sparkContext.parallelize(expanded).toJavaRDD()
    new JavaPairRDD[String, ProductDesc](ret)
  }

  /**
   * Computes the clusters given a set of product descriptions. This method will be called on the same Algorithm instance
   * for different bootstraps and parameter configurations. You can implement internal caching. However, make sure that
   * you do use too much memory. I suggest using a cache that is bounded (e.g., using the Google Guava Cache library).
   * @param conf the configuration of parameters to use
   * @param data the product descriptions
   * @return a Clusters object (you can create one using the ClustersBuilder class)
   */
  override def computeClusters(conf: java.util.Map[String, Any], data: JavaRDD[ProductDesc]): JavaPairRDD[String, ProductDesc]

}

trait SparkAlgorithmFactory extends Serializable {
  def build(): SparkAlgorithm
}