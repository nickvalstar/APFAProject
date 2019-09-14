package com.damirvandic.sparker.msm

import java.io.ObjectOutputStream
import java.util

import com.damirvandic.sparker.core.ParamParser
import com.damirvandic.sparker.data.{DataSet, Reader}
import com.damirvandic.sparker.msm.MsmSimCaching._
import com.damirvandic.sparker.util.IntPair
import gnu.trove.map.hash.TObjectFloatHashMap
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.io.BufferedSource
import scala.util.Random

case class CachingInfo(var _basePath: String, prefix: String) {
  val basePath = if (!_basePath.endsWith("/")) _basePath + "/" else _basePath
}

object MsmSimCaching {
  private val defaultArgs = Array(
    "-p:alpha=d 0.6 0.8 0.3",
    "-p:beta=d 0.0",
    "-p:gamma=d 0.75",
    //    "-p:epsilon=d 0.52",
    "-p:mu=d 0.65",
    "-p:kappa=d 1.0",
    "-p:lambda=d 1.0 2.0 3.0",
    "-p:brandHeuristic=b true",
    "./hdfs/TVs-1000.json",
    "./hdfs/caching"
  )

  def main(args: Array[String]) {
    val runner = Runner(if (args.isEmpty) defaultArgs else args)
    runner.run()
  }

  case class Config(outputPath: String = "",
                    prefix: String = "msmSims__",
                    numSlicesParams: Int = -1,
                    params: Seq[(String, String)] = Seq.empty,
                    dsPath: String = "") {
    def summary: String = {
      s"""
        |
        |Using the following settings:
        |- outputPath = $outputPath
        |- numSlicesParams = $numSlicesParams
      """.stripMargin
    }
  }

}

case class Runner(args: Array[String]) {

  val sc = new SparkContext(new SparkConf().setAppName("Sparker - MSM caching"))
  val c: MsmSimCaching.Config = parseConfig(args, sc)
  private val logger = LoggerFactory.getLogger(getClass)

  def run(): Unit = {
    logger.info(c.summary)
    val dataSet = readData()
    val bcDataSet = sc.broadcast(dataSet)
    val grid = ParamParser.getParams(c.params)
    logger.info("\n\nThe following parameter grid is specified:\n{}\n", grid.stringRepr)
    val parameters = sc.parallelize(grid.iterator.toSeq, numSlices = c.numSlicesParams)
    val simMaps = parameters.map(conf => new SimRunner(conf, bcDataSet).compute).cache()
    logger.info("There are {} sims computed, starting collection", simMaps.count())
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = createResultsFolder(fs, c.outputPath)
    for ((confKey, simMap) <- simMaps.toLocalIterator) {
      logger.info("Collected sim map for key {}", confKey)
      val stream = fs.create(new Path(path + c.prefix + confKey + ".bin"))
      val out = new ObjectOutputStream(stream)
      out.writeObject(simMap)
      out.close()
    }
  }

  private def parseConfig(args: Array[String], sc: SparkContext): Config = {
    Parser.parser.parse(args.toSeq, Config(numSlicesParams = sc.defaultParallelism)) getOrElse {
      System.exit(1)
      Config() // just to make the compiler happy)
    }
  }

  private def readData() = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val in = fs.open(new Path(c.dsPath))
    Reader.readDataSet(c.dsPath.substring(c.dsPath.lastIndexOf('/') + 1, c.dsPath.lastIndexOf('.')), new BufferedSource(in))
  }

  private def createResultsFolder(fs: FileSystem, origPath: String) = {
    val newPath = getAbsolutePath(ensureOutpath(fs, new Path(origPath)))
    val base = if (!origPath.startsWith("/") && newPath.startsWith("/")) newPath.drop(1) else newPath
    val trailingSlash = if (!base.endsWith("/")) base + "/" else base
    trailingSlash.replace("//", "/")
  }

  private def getAbsolutePath(path: Path): String = {
    if (path == null) ""
    else
      path.getParent match {
        case null => ""
        case p: Path => getAbsolutePath(p.getParent) + "/" + p.getName + "/" + path.getName
      }
  }

  private def ensureOutpath(fs: FileSystem, path: Path, prefix: String = "results") = {
    val outPath = path
    val out = if (fs.exists(outPath)) {
      val ret = new Path(outPath.getParent, "caching_" + Random.alphanumeric.take(30).mkString)
      fs.mkdirs(ret)
      logger.warn(s"Path ${outPath.getName} already exists, using path ${ret.getName}")
      ret
    } else {
      fs.mkdirs(outPath)
      outPath
    }
    out
  }

}

class SimRunner(conf: Map[String, Any], bcDataSet: Broadcast[DataSet]) {

  import scala.collection.convert.decorateAll._

  val logger = LoggerFactory.getLogger(getClass)

  def compute = {
    val simMap = new TObjectFloatHashMap[IntPair]()
    val simmer = getSimComputer(conf)
    val data = bcDataSet.value
    val vector = data.descriptions.toArray
    val n = vector.size
    logger.info("Have to do {} sims", n * (n - 1) / 2)
    var counter = 1
    for (i <- 0 until n) {
      val a = vector(i)
      for (j <- (i + 1) until n) {
        val b = vector(j)
        val sim = simmer.computeSim(a, b).toFloat // this will update the cache
        simMap.put(new IntPair(a.ID, b.ID), sim)
        if (counter % 1000 == 0) {
          logger.info("sim computation progress: n = {}", counter)
        }
        counter += 1
      }
    }
    val msmFilteredMap = MsmConfig.filterCacheEffectiveKeys(conf.asJava.asInstanceOf[util.Map[String, AnyRef]])
    (CacheConfKey.get(msmFilteredMap), simMap)
  }

  private def getSimComputer(conf: Map[String, Any]) = {
    val alpha: Double = conf("alpha").asInstanceOf[Double]
    val beta: Double = conf("beta").asInstanceOf[Double]
    val kappa: Double = conf("kappa").asInstanceOf[Double]
    val lambda: Double = conf("lambda").asInstanceOf[Double]
    val gamma: Double = conf("gamma").asInstanceOf[Double]
    val mu: Double = conf("mu").asInstanceOf[Double]
    val brandHeuristic: Boolean = conf("brandHeuristic").asInstanceOf[Boolean]

    val c: util.Map[String, java.lang.Double] = getConfig(alpha, beta, kappa, lambda, gamma, mu)
    new MsmSimilarity(brandHeuristic, c, new MsmKeyMatcher(gamma), null) // no cache
  }

  private def getConfig(alpha: Double, beta: Double, kappa: Double, lambda: Double, gamma: Double, mu: Double): util.Map[String, java.lang.Double] = {
    val ret = new util.HashMap[String, java.lang.Double]
    ret.put("alpha", alpha)
    ret.put("beta", beta)
    ret.put("kappa", kappa)
    ret.put("lambda", lambda)
    ret.put("gamma", gamma)
    ret.put("mu", mu)
    java.util.Collections.unmodifiableMap(ret)
  }
}

object CacheConfKey {

  import scala.collection.convert.decorateAll._

  def get(conf: Map[String, Any]) = {
    conf.keySet.toIndexedSeq.sorted.map(k => s"${k}_${conf(k).toString.toLowerCase}").mkString("__")
  }

  def get(jConf: java.util.Map[String, AnyRef]) = {
    val conf = jConf.asScala
    conf.keySet.toIndexedSeq.sorted.map(k => s"${k}_${conf(k).toString.toLowerCase}").mkString("__")
  }
}

private object Parser {
  val parser = new scopt.OptionParser[MsmSimCaching.Config]("sparker") {
    head("msm sim pre-computer", "1.x")

    opt[String]("prefix") abbr "pfx" action { (x, c) =>
      c.copy(prefix = x)
    } text "prefix with which the file name of the cache map will be prepended"

    opt[Int]("numSlicesParams") abbr "sp" action { (x, c) =>
      c.copy(numSlicesParams = x)
    } text "number of parameter slices to use (default = # executors on Spark Cluster)"

    opt[(String, String)]('p', "params") optional() unbounded() action {
      (x, c) => c.copy(params = c.params :+ x)
    } text
      """
        |defines a parameter (can be used multiple times), where key is the parameter name and the value can be one of the following (examples)
        |        - "i 0 1 2 3" in case of fixed integer set
        |        - "ri 0 10 1" in case of integer range (0 to 10 with step size of 1)
        |        - "d 0.3 0.4 0.6" in case of fixed double set
        |        - "rd 0 1 0.05" in case of double range (0 to 1 with step size of 0.05)
        |        - "b true false" in case of boolean range
        |        - "s A B C D" in case of fixed string set
      """.stripMargin.drop(1) //remove first enter

    arg[String]("<data set path>") required() action { (x, c) =>
      c.copy(dsPath = x)
    } text "path to the data set JSON"

    arg[String]("<output path>") required() action { (x, c) =>
      c.copy(outputPath = x)
    } text "output path for results (will be written to HDFS if available)"
  }
}