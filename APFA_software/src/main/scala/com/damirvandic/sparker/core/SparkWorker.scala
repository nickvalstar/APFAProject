package com.damirvandic.sparker.core

import java.util.UUID

import com.damirvandic.sparker.algorithm.{Algorithm, AlgorithmFactory}
import com.damirvandic.sparker.data.DataSet
import com.damirvandic.sparker.eval._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

import scala.util.Random

class SparkWorker {

  private val ID = UUID.randomUUID().toString
  private val logger = LoggerFactory.getLogger(getClass)

  def computeResult(hadoopConfig: java.util.Map[String, String],
                    cachePath: String,
                    seed: Long,
                    numBootstraps: Int,
                    parameterConfigsIt: Iterator[Map[String, Any]],
                    bcDataSet: Broadcast[DataSet],
                    algorithmFactoryCls: String): JobResult = {
    val parameterConfigs = parameterConfigsIt.toSeq
    logger.info("Started SparkWorker {} with {} parameters", ID, parameterConfigs.size)
    val dataSet = bcDataSet.value
    val sampler: Sampler = new SamplerImpl(seed)
    val alg = Class.forName(algorithmFactoryCls).newInstance().asInstanceOf[AlgorithmFactory].build(cachePath, hadoopConfig)
    val results = (1 to numBootstraps).map(bootstrapID => {
      val sample = sampler.sample(dataSet)

      val trainSize = sample.train.size
      val testSize = sample.test.size
      val (bestConfig, trainResult, trainSetClusters, trainDur, trainVarMap) = findBestAlgorithm(alg, parameterConfigs, sample.train, trainSize, sample.trainClusters)

      alg.clearAllVariables()
      val startTest = System.nanoTime()
      val testSetClusters = alg.computeClusters(bestConfig, sample.test)
      val testDur = ((System.nanoTime() - startTest) / 1E6).toLong

      alg.registerVariable("inputSize", testSize.toString)
      val testVarMap = alg.variableMap.toMap

      val testResult = new ClustersEvalImpl().evaluate(testSetClusters, sample.testClusters)

      val clustersStrTrain = ResultsWriter.clusters(trainSetClusters)
      val clustersStrTest = ResultsWriter.clusters(testSetClusters)
      logger.info("Done with bootstrap " + bootstrapID)
      BootstrapResult(alg.name, bootstrapID, bestConfig, trainResult, testResult, clustersStrTrain, clustersStrTest, trainDur, testDur, trainVarMap, testVarMap)
    })
    JobResult(alg.name, results)
  }

  private def findBestAlgorithm(alg: Algorithm, parameterConfigs: Seq[Map[String, Any]], data: Set[ProductDesc], dataSize: Long, correctClusters: Clusters) = {
    var bestF1 = Double.MinValue
    var bestConfig: Map[String, Any] = Map.empty
    var result: Option[Results] = None
    var bestDur = 0l
    var bestClusters: Clusters = NoClusters
    var bestVarMap: Map[String, String] = Map.empty

    for (paramConfig <- parameterConfigs) {

      alg.clearAllVariables()
      val startTrain = System.nanoTime()
      val computedClusters = alg.computeClusters(paramConfig, data)
      val trainDur = ((System.nanoTime() - startTrain) / 1E6).toLong
      val res = new ClustersEvalImpl().evaluate(computedClusters, correctClusters)
      if (bestF1 < res.f1) {
        bestF1 = res.f1
        bestConfig = paramConfig
        result = Some(res)
        bestDur = trainDur
        bestClusters = computedClusters
        alg.registerVariable("inputSize", dataSize.toString)
        bestVarMap = alg.variableMap.toMap
      }
    }

    (bestConfig, result.get, bestClusters, bestDur, bestVarMap)
  }
}
