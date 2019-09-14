package com.damirvandic.sparker.data

import com.damirvandic.sparker.core.{Clusters, ClustersBuilder, ProductDesc}
import com.damirvandic.sparker.util.SafeCounter

import scala.collection.Map
import scala.collection.convert.decorateAll._


case class DataSet(name: String, descriptions: Set[ProductDesc], clusters: Clusters) {
  def jDescriptions: java.util.Set[ProductDesc] = descriptions.asJava
}

object DataSet {
  /**
   * Creates an inverted index so that clusters can be looked up efficiently
   * for a particular product descriptions.
   * @param clusters
   * @return
   */
  def createIndex(clusters: Set[Set[ProductDesc]]): Map[ProductDesc, Set[ProductDesc]] = {
    clusters.flatMap(cluster => cluster.map(p => (p, cluster))).toMap
  }

  def merge(startDS: DataSet, datasets: List[DataSet]): DataSet = datasets.foldLeft(startDS)(DataSet.merge)

  def merge(ds1: DataSet, ds2: DataSet): DataSet = {
    assert(ds1.name == ds2.name)
    val size = (ds: DataSet) => ds.descriptions.size
    val (ds_larger, ds_smaller) = if (size(ds1) > size(ds2)) (ds1, ds2) else (ds2, ds1)
    var ds_pointer = ds_larger

    def addPD(ds: DataSet, shop: String, modelID: String, pd: ProductDesc): DataSet = {
      val descsNew = ds.descriptions + pd
      val clusters = ds.clusters.asSet
      val clustersNew = clusters.find(c => c.head.modelID == modelID) match {
        case Some(cluster) => clusters - cluster + (cluster + pd)
        case None => clusters + Set(pd)
      }
      DataSet(ds.name, descsNew, ClustersBuilder().fromSets(clustersNew))
    }

    ds_smaller.clusters.foreach(cluster => {
      val shop = cluster.head.shop
      val modelID = cluster.head.modelID
      cluster.foreach(pd => {
        ds_pointer = addPD(ds_pointer, shop, modelID, pd)
      })
    })
    ds_pointer
  }
}

object PD {
  def apply(title: String, modelID: String, url: String, featuresMap: Map[String, String]) = new ProductDesc(SafeCounter.next, title, modelID, url, featuresMap.asJava)
}