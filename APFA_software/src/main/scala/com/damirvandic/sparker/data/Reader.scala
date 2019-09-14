package com.damirvandic.sparker.data

import com.damirvandic.sparker.core
import com.damirvandic.sparker.core.ClustersBuilder
import com.damirvandic.sparker.util.SafeCounter
import spray.json._

import scala.collection.JavaConversions._
import scala.io.{BufferedSource, Source}

object Reader {
  val SKIP_PROPERTIES = Set("model", "title", "item model number", "model number")

  def readDataSet(dataSetName: String, source: BufferedSource) = {
    val strInput = source.mkString
    source.close()
    parseDataSet(dataSetName, strInput)
  }

  def readDataSet(datasetName: String, file: String): DataSet = {
    val strInput = Source.fromFile(file).mkString
    parseDataSet(datasetName, strInput)
  }

  private def parseDataSet(datasetName: String, strInput: String): DataSet = {
    val json = strInput.parseJson
    val rawDataSet = extractRaw(json)
    val clusters = filterInvalid(rawDataSet)
    val descriptions = clusters.flatMap(_.toTraversable)
    DataSet(datasetName, descriptions, ClustersBuilder().fromSets(clusters))
  }

  private def filterInvalid(rawDataSet: Set[Set[core.ProductDesc]]) = {
    implicit def bool2int(b: Boolean) = if (b) 1 else 0
    def filterInvalid(descs: Set[core.ProductDesc]): Set[core.ProductDesc] =
      descs.filter(p => {
        val m = p.featuresMap
        m.size > m.contains("title").toInt + m.contains("model").toInt
      }).map(p => {
        val cleanedMap = p.featuresMap.filterNot(e => e._2 == "" || SKIP_PROPERTIES.contains(e._1.toLowerCase))
        PD(p.title, p.modelID, p.url, cleanedMap)
      })

    rawDataSet.map(filterInvalid).filter(_.size > 0)
  }

  private def extractRaw(value: JsValue) = {
    value.asJsObject.fields.map(v => v._2 match {
      case descs: JsArray => {
        descs.elements.map(_.convertTo[core.ProductDesc](CustomJsonProtocols.reader)).toSet
      }
      case _ => deserializationError("Error extracting data set for:\n" + v._2.prettyPrint)
    }).toSet
  }
}

object CustomJsonProtocols extends DefaultJsonProtocol {
  implicit val reader = JsonReader.func2Reader(ProductDescJsonFormat.read)

  object ProductDescJsonFormat extends JsonFormat[core.ProductDesc] {
    def write(p: core.ProductDesc) = JsObject(
      Map(
        "title" -> JsString(p.title),
        "modelID" -> JsString(p.modelID),
        "url" -> JsString(p.modelID),
        "shop" -> JsString(p.modelID),
        "featuresMap" -> p.featuresMap.toMap.toJson
      )
    )

    def read(value: JsValue) = {
      try {
        val fields = value.asJsObject.fields
        val title = fields.get("title").get.convertTo[String]
        val modelID = fields.get("modelID").get.convertTo[String]
        val featuresMap = fields.get("featuresMap").get.convertTo[Map[String, String]]
        val url = fields.get("url").map(_.convertTo[String]).orElse(featuresMap.get("url")).get
        new core.ProductDesc(SafeCounter.next, title, modelID, url, featuresMap)
      } catch {
        case e: Throwable => throw new DeserializationException("Error extracting product description for:\n" + value.prettyPrint, e)
      }
    }
  }


}

