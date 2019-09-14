package com.damirvandic.sparker

import com.damirvandic.sparker.data.Reader
import org.scalatest._

import scala.io.Source

//import scala.collection.JavaConversions._

class ReaderSpec extends FlatSpec with ShouldMatchers {
  "The Reader" should "correctly read a JSON data set file" in {
    val src1 = Source.fromURL(getClass.getResource("/test_data.json"))
    val d1 = Reader.readDataSet("test1", src1)
    d1.descriptions.groupBy(_.shop).size should be(2) // there should be 2 shops
    d1.descriptions.size should be(5) // there should be 5 descriptions in total
    d1.clusters.clusterCount should be(3)

    // check if product description filtering works
    val src2 = Source.fromURL(getClass.getResource("/test_data2.json"))
    val d2 = Reader.readDataSet("test2", src2)
    d2.descriptions.groupBy(_.shop).size should be(1) // just one shop
    d2.descriptions.size should be(2) // two descriptions
    d2.clusters.clusterCount should be(1) // 1 cluster

    // check if cleaning of feature map works
    val head = d2.descriptions.head
    head.featuresMap.containsKey("title") should be(false)
    head.featuresMap.containsKey("model") should be(false)

    // check shop
    head.shop should be("newegg.com")
  }
}
