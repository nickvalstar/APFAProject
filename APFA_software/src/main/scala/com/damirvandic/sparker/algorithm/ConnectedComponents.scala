package com.damirvandic.sparker.algorithm

import java.util.UUID

import com.damirvandic.sparker.core.ProductDesc
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

trait BlockingFunction[T] {
  def isBlocking(a: T, b: T): Boolean
}

object BlockingFunction {
  def empty[T]: BlockingFunction[T] = new BlockingFunction[T] {
    def isBlocking(a: T, b: T): Boolean = false
  }
}

object ProductDescBlockingFunction extends BlockingFunction[ProductDesc] {
  override def isBlocking(a: ProductDesc, b: ProductDesc): Boolean = a.shop.equals(b.shop)
}

trait ConnectedComponents {
  /**
   * @param data the edges
   * @tparam T type of item (e.g., ProductDesc)
   * @return (CLUSTER_ID, ITEM)
   */
  def findComponents[T: ClassTag](data: RDD[(T, T)], bf: BlockingFunction[T]): RDD[(String, T)]

  def findComponents[T: ClassTag](data: Seq[(T, T)], bf: BlockingFunction[T]): Set[(String, T)]
}

class ConnectedComponentsImpl extends ConnectedComponents {
  private val log = LoggerFactory.getLogger(getClass)

  override def findComponents[T: ClassTag](rawData: RDD[(T, T)], bf: BlockingFunction[T]): RDD[(String, T)] = {
    rawData.persist()
    log.debug("There are {} tuples", rawData.count())

    val mappings = rawData.context.broadcast(rawData.flatMap(t => Seq(t._1.toString -> t._1, t._2.toString -> t._2)).collectAsMap())
    val data = rawData.map(t => (t._1.toString, t._2.toString))

    log.debug("computing adjency list")
    val adj_list = data.flatMap({ case (x, y) => Seq(x -> Set(x, y), y -> Set(x, y))}).persist()
    log.debug("adjency list computed: {}", adj_list.count())

    var prevClusters = find(adj_list)
    var currClusters = find(prevClusters)

    while (notSame(prevClusters, currClusters)) {
      prevClusters.unpersist()
      prevClusters = currClusters
      currClusters = find(prevClusters)
    }

    val clusters = currClusters.map(t => (t._2, t._1)).groupByKey().flatMap({ case (cluster, items) => {
      val clusterID = UUID.randomUUID().toString
      items.map(i => (clusterID, i))
    }
    })

    prevClusters.unpersist()
    currClusters.unpersist()
    rawData.unpersist()

    clusters.map(t => (t._1, mappings.value(t._2)))
  }

  private def notSame[T: ClassTag](prevClusters: RDD[(T, Set[T])], currClusters: RDD[(T, Set[T])]): Boolean = {
    prevClusters
      .join(currClusters)
      .filter({ case (i, (prevCluster, currCluster)) => prevCluster != currCluster})
      .count() > 0
  }

  private def find[T: ClassTag](data: RDD[(T, Set[T])]): RDD[(T, Set[T])] = {
    val expanded: RDD[(T, Set[T])] = data.mapPartitions(tuples => {
      val map = scala.collection.mutable.Map.empty[T, mutable.Set[T]]
      for (t <- tuples) {
        val neighbours = t._2
        for (n <- neighbours) {
          // ensure map
          val m = if (map.contains(n)) {
            map(n)
          } else {
            val m = scala.collection.mutable.Set.empty[T]
            map.put(n, m)
            m
          }
          // add all neighbours
          m ++= neighbours
        }
      }
      map.mapValues(_.toSet).iterator
    })
    val ret = expanded.reduceByKey(_ union _).persist()
    log.debug("After reduce: {} ({} elements)", ret.count(), ret.collect().map(_._2.size).sum)
    ret
  }

  private def findComponent[T](bf: BlockingFunction[T],
                               visited: scala.collection.mutable.Set[T],
                               vertices: scala.collection.mutable.Set[T],
                               map: Map[T, Set[T]]): Set[T] = {
    val start = vertices.head
    val c = scala.collection.mutable.Set.empty[T]

    def traverse(v: T): Unit = {
      if (!c.exists(s => bf.isBlocking(s, v))) {
        c.add(v) // add element itself
        visited.add(v)
        vertices.remove(v) // remove from traversing in the future
        for (w <- map(v); if !c.contains(w) && !visited.contains(w)) {
          traverse(w)
        }
      }
    }
    traverse(start)

    c.toSet
  }

  override def findComponents[T: ClassTag](data: Seq[(T, T)], bf: BlockingFunction[T]): Set[(String, T)] = {
    val start = System.nanoTime()
    val adjList = getAdjacencyList(data)
    val visited = scala.collection.mutable.Set.empty[T]
    val vertices = scala.collection.mutable.Set.empty[T] ++ adjList.keySet
    val components = scala.collection.mutable.Set.empty[Set[T]]
    while (vertices.nonEmpty) {
      val c = findComponent(bf, visited, vertices, adjList)
      components.add(c)
    }
    val ret = components.flatMap(cluster => {
      val ID = UUID.randomUUID().toString
      cluster.map((ID, _))
    }).toSet
    val end = System.nanoTime()
    log.debug("Found connected components in {} ms", (end - start) / 1e6)
    ret
  }

  def   findComponents(data: java.util.Set[(ProductDesc, ProductDesc)]): java.util.Set[java.util.Set[ProductDesc]] = {
    import scala.collection.convert.decorateAll._
    val ret = findComponents(data.asScala.toSeq, ProductDescBlockingFunction).groupBy(_._1).mapValues(_.map(_._2).asJava).values.toSet
    ret.asJava
  }

  private def getAdjacencyList[T: ClassTag](data: Seq[(T, T)]): Map[T, Set[T]] = {
    val ret = scala.collection.mutable.Map.empty[T, mutable.Set[T]]
    def ensure(v: T): mutable.Set[T] = {
      if (ret.contains(v)) ret(v)
      else {
        val s = scala.collection.mutable.Set.empty[T]
        ret.put(v, s)
        s
      }
    }
    for ((v1, v2) <- data) {
      val s1 = ensure(v1)
      val s2 = ensure(v2)
      s1.add(v2)
      s2.add(v1)
    }
    ret.mapValues(_.toSet).toMap
  }
}
