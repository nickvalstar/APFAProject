package com.damirvandic.sparker.core

import java.util
import java.util.Locale

import com.damirvandic.sparker.algorithm.{AlgorithmFactory, ParameterGrid, ParameterGridBuilder, SparkAlgorithmFactory}
import com.damirvandic.sparker.data.{DataSet, Reader}
import com.damirvandic.sparker.eval.{RDDSample, Sample, Sampler, SamplerImpl}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.io.BufferedSource
import scala.util.{Failure, Success, Try}

case class Config(seed: Long = 2566864879360712787l,
                  outputPath: String = "",
                  numBootstraps: Int = 1,
                  numChunksParams: Int = 10,
                  numSlicesData: Int = 1,
                  numSlicesParams: Int = -1,
                  params: Seq[(String, String)] = Seq.empty,
                  factoryCls: String = "",
                  dsName: String = "",
                  dsPath: String = "",
                  cachePath: String = "") {
  def summary: String = {
    s"""
        |
        |Using the following settings:
        |- seed = $seed
        |- outputPath = $outputPath
        |- numBootstraps = $numBootstraps
        |- numChunksParams = $numChunksParams
        |- numSlicesData = $numSlicesData
        |- numSlicesParams = $numSlicesParams
      """.stripMargin
  }
}

object Parser {
  val log = LoggerFactory.getLogger(getClass)

  val parser = new scopt.OptionParser[Config]("sparker") {
    head("sparker", "1.x")
    opt[Long]('s', "seed") action { (x, c) =>
      c.copy(seed = x)
    } text "random seed used for bootstrap generation (default = 2566864879360712787)"

    opt[Int]('b', "bootstraps") action { (x, c) =>
      c.copy(numBootstraps = x)
    } text "number of bootstraps to use (default = 1)"

    opt[Int]("numChunksParams") abbr "ch" action { (x, c) =>
      c.copy(numChunksParams = x)
    } text "number of parameter chunks to use (default = 10)"

    opt[Int]("numSlicesData") abbr "sd" action { (x, c) =>
      c.copy(numSlicesData = x)
    } text "number of data slices to use in case of a distributed algorithm (default = 1)"

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

    arg[String]("<algorithm factory class>") required() action { (x, c) =>
      log.info("debug: input factory = {}", x)

      c.copy(factoryCls = x)
    } text "fully qualified name of algorithm factory class"

    arg[String]("<data set name>") required() action { (x, c) =>
      c.copy(dsName = x)
    } text "name of the data set"

    arg[String]("<data set path>") required() action { (x, c) =>
      c.copy(dsPath = x)
    } text "path to the data set JSON"

    arg[String]("<output path>") required() action { (x, c) =>
      c.copy(outputPath = x)
    } text "output path for results (will be written to HDFS if available)"

    arg[String]("<cache path>") required() action { (x, c) =>
      c.copy(cachePath = x)
    } text "path where cache files are located (path can be empty but must exist)"
  }
}

object MainRunner {
  def main(args: Array[String]) {
    val runner = new SparkRunner(args)
    runner.run()
    runner.stop()
  }
}

class SparkRunner(val args: Array[String]) {
  val logger = LoggerFactory.getLogger(getClass)
  logger.info("debug: input args = {}", args)
  val sc = new SparkContext(new SparkConf().setAppName("Sparker"))
  val c = Parser.parser.parse(args.toSeq, Config(numSlicesParams = sc.defaultParallelism)) getOrElse {
    System.exit(1)
    Config() // just to make the compiler happy)
  }
  Locale.setDefault(Locale.US)

  def stop(): Unit = sc.stop()

  def run() = {
    logger.info(c.summary)
    val start = System.nanoTime()

    val (paramChunks, parameterCount) = parallelizeParams

    val dataSet = readData()

    logger.info(
      s"""
        |
        |Overview of job:
        |- ${c.factoryCls}
        |- ${c.numBootstraps} bootstraps
        |- $parameterCount parameters to evaluate
        |- ${paramChunks.size} parameter chunks
        |- sizes of chunks: [${paramChunks.map(_.size).mkString(", ")}]
      """.stripMargin)

    var currentReduced: JobResult = null

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val writer = new ResultsWriter(fs, c.outputPath)
    writer.writeSeed(c.seed)
    for ((paramsChunk, i) <- paramChunks.zipWithIndex) {
      logger.info("Started evaluating parameter chunk {} ({} parameters)", i, paramsChunk.size)

      val rddParamsChunk = sc.parallelize(paramsChunk, c.numSlicesParams)
      val resultsRDD = computeJobResult(rddParamsChunk, dataSet, sc)
      val reducedResult = resultsRDD.reduce({
        case (x, y) => SparkResultsAggregator.aggregate(x, y)
      })

      if (currentReduced == null) {
        currentReduced = reducedResult
      } else {
        currentReduced = SparkResultsAggregator.aggregate(reducedResult, currentReduced)
      }

      writer.write(i, c.seed, currentReduced.results)
      val n = currentReduced.results.size.toDouble
      val f1 = currentReduced.results.foldLeft(0.0)((o, b) => o + b.testResuslt.f1) / n
      logger.info("Finished evaluating parameter chunk {} (average F1 = {})", i, f1)
    }

    val end = System.nanoTime()
    logger.info(s"Done with run, it took {} ms", (end - start) / 1e6)

    logger.info(s"Closing Spark Client")
  }

  def readData() = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val in = fs.open(new Path(c.dsPath))
    Reader.readDataSet(c.dsName, new BufferedSource(in))
  }

  private def computeJobResult(parameterConfigs: RDD[Map[String, Any]],
                               dataSet: DataSet,
                               sc: SparkContext): RDD[JobResult] = {
    Try(Class.forName(c.factoryCls)) match {
      case Success(cls) => cls.newInstance() match {
        case x: AlgorithmFactory => sparkWorker(parameterConfigs, dataSet, c.factoryCls)
        case x: SparkAlgorithmFactory => distributedSparkWorker(parameterConfigs, dataSet, c.factoryCls)
        case _ => throw new IllegalArgumentException(s"Class ${c.factoryCls} is not an algorithm factory!")
      }
      case Failure(ex) => throw new IllegalArgumentException(s"Factory class ${c.factoryCls} not found!")
    }
  }

  private def distributedSparkWorker(parameterConfigs: RDD[Map[String, Any]],
                                     dataSet: DataSet,
                                     algorithmFactoryCls: String): RDD[JobResult] = {
    val rddSamples = computeSamples(dataSet).map(s => {
      val numSlicesData = c.numSlicesData
      val rddTrain = sc.parallelize(s.train.toSeq, numSlicesData).cache()
      val rddTest = sc.parallelize(s.test.toSeq, numSlicesData).cache()
      RDDSample(rddTrain, rddTest, s.trainClusters, s.testClusters)
    })
    val bcRddSamples = sc.broadcast(rddSamples)
    parameterConfigs.mapPartitions(params =>
      if (params.isEmpty) {
        Iterator.empty
      } else {
        Iterator(new SparkDistributedWorker().computeResult(params, bcRddSamples, algorithmFactoryCls))
      }
    )
  }

  private def computeSamples(ds: DataSet): Seq[Sample] = {
    val sampler: Sampler = new SamplerImpl(c.seed)
    (1 to c.numBootstraps).map(_ => sampler.sample(ds))
  }

  private def sparkWorker(parameterConfigs: RDD[Map[String, Any]],
                          dataSet: DataSet,
                          algorithmFactoryCls: String): RDD[JobResult] = {
    val bcDataSet = sc.broadcast(dataSet)
    val numBootstraps = c.numBootstraps
    val hadoopConfiguration = getHadoopConf(sc)
    val seed = c.seed
    val cachePath = c.cachePath
    parameterConfigs.mapPartitions(params =>
      if (params.isEmpty) {
        Seq.empty[JobResult].toIterator
      } else {
        Iterator(new SparkWorker().computeResult(hadoopConfiguration, cachePath, seed, numBootstraps, params, bcDataSet, algorithmFactoryCls))
      }
    )
  }

  private def getHadoopConf(sc: SparkContext): java.util.Map[String, String] = {
    val ret = new util.HashMap[String, String]()
    val it = new JavaSparkContext(sc).hadoopConfiguration.iterator()
    while (it.hasNext) {
      val entry = it.next()
      ret.put(entry.getKey, entry.getValue)
    }
    ret
  }

  private def parallelizeParams: (Seq[IndexedSeq[Map[String, Any]]], Int) = {
    val grid = ParamParser.getParams(c.params)
    logger.info("\n\nThe following parameter grid is specified:\n{}\n", grid.stringRepr)
    val parameters = grid.iterator.toIndexedSeq
    val parameterCount = parameters.size
    val paramChunks = cut(parameters, c.numChunksParams)
    (paramChunks.toSeq.filterNot(_.isEmpty), parameterCount)
  }

  private def cut[A](xs: IndexedSeq[A], maxSplits: Int) = {
    val n = if (maxSplits > xs.size) xs.size else maxSplits
    val (quot, rem) = (xs.size / n, xs.size % n)
    val (smaller, bigger) = xs.splitAt(xs.size - rem * (quot + 1))
    smaller.grouped(quot) ++ bigger.grouped(quot + 1)
  }
}

object ParamParser {
  def getParams(tuples: Seq[(String, String)]): ParameterGrid = {
    val builder = new ParameterGridBuilder
    for ((paramName, paramRange) <- tuples) {
      builder.addRange(paramName, getParameterArray(paramRange))
    }
    builder.build()
  }

  private def getParameterArray(strRangeRaw: String): Array[Any] = {
    val strRange = strRangeRaw.trim
    val vals = strRange.trim.split(" +").drop(1)
    def error = throw new IllegalArgumentException(s"$strRangeRaw is an invalid parameter range specification")
    val ret: Seq[Any] = strRange.split(" ")(0).toLowerCase match {
      case "i" => vals.map(_.toInt).toSeq
      case "ri" => vals.map(_.toInt).toSeq match {
        case Seq(s, e, i) => Range.inclusive(s, e, i).toArray.toSeq
        case _ => error
      }
      case "d" => vals.map(_.toDouble).toSeq
      case "rd" => vals.map(_.toDouble).toSeq match {
        case Seq(s, e, i) => Range.Double.inclusive(s, e, i).toArray.toSeq
        case _ => error
      }
      case "b" => vals.map(_.toBoolean).toSeq
      case "s" => vals
      case _ => error
    }
    ret.toArray
  }
}