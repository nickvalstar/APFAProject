package com.damirvandic.sparker.core

import java.util.UUID

import com.damirvandic.sparker.algorithm.{SparkAlgorithm, SparkAlgorithmFactory}
import com.damirvandic.sparker.eval._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

class SparkDistributedWorker {

  private val logger = LoggerFactory.getLogger(getClass)
  private val ID = UUID.randomUUID().toString

  def computeResult(parameterConfigsIt: Iterator[Map[String, Any]], bcRddSamples: Broadcast[Seq[RDDSample]], algorithmFactoryCls: String): JobResult = {
    val parameterConfigs = parameterConfigsIt.toSeq
    logger.info("Started SparkDistributedWorker {} with {} parameters", ID, parameterConfigs.size)
    val samples = bcRddSamples.value
    val alg = Class.forName(algorithmFactoryCls).newInstance().asInstanceOf[SparkAlgorithmFactory].build()
    val results = samples.zipWithIndex.map({
      case (sample, bootstrapID) => {
        val dataSizeTrain = sample.train.count()
        val dataSizeTest = sample.test.count()

        val (bestConfig, trainResult, collectedTrain, trainDur, trainVarMap) = findBestAlgorithm(alg, parameterConfigs, sample.train, dataSizeTrain, sample.trainClusters)

        alg.clearAllVariables()
        val startTest = System.nanoTime()
        val testSetClustersRDD = alg.computeClusters(bestConfig, sample.test)
        val collectedTest = testSetClustersRDD.collect()
        val testDur = ((System.nanoTime() - startTest) / 1E6).toLong

        alg.registerVariable("inputSize", dataSizeTest.toString)
        val testVarMap = alg.variableMap.toMap

        val testSetClusters = collectClusters(collectedTest)
        val testResult = new ClustersEvalImpl().evaluate(testSetClusters, sample.testClusters)

        logger.debug(s"Best config for bootstrap $bootstrapID => $bestConfig")
        val clustersStrTrain = ResultsWriter.clusters(collectedTrain)
        val clustersStrTest = ResultsWriter.clusters(collectedTest)
        BootstrapResult(alg.name, bootstrapID, bestConfig, trainResult, testResult, clustersStrTrain, clustersStrTest, trainDur, testDur, trainVarMap, testVarMap)
      }
    })
    JobResult(alg.name, results.toIndexedSeq)
  }

  private def findBestAlgorithm(alg: SparkAlgorithm, parameterConfigs: Seq[Map[String, Any]], data: RDD[ProductDesc], dataSize: Long, correctClusters: Clusters) = {
    var bestF1 = Double.MinValue
    var bestConfig: Map[String, Any] = Map.empty
    var bestDur: Long = 0
    var bestClusters: Array[(String, ProductDesc)] = Array.empty
    var bestVarMap: Map[String, String] = Map.empty
    var result: Option[Results] = None

    for (paramConfig <- parameterConfigs) {
      alg.clearAllVariables()
      val startTrain = System.nanoTime()
      val computedClustersRDD = alg.computeClusters(paramConfig, data)
      val collectedTrain = computedClustersRDD.collect()
      val trainDur = ((System.nanoTime() - startTrain) / 1E6).toLong

      val computedClusters = collectClusters(collectedTrain)
      val res = new ClustersEvalImpl().evaluate(computedClusters, correctClusters)
      if (bestF1 < res.f1) {
        bestF1 = res.f1
        bestConfig = paramConfig
        result = Some(res)
        bestDur = trainDur
        bestClusters = collectedTrain
        alg.registerVariable("inputSize", dataSize.toString)
        bestVarMap = alg.variableMap.toMap
      }
    }

    (bestConfig, result.get, bestClusters, bestDur, bestVarMap)
  }

  private def collectClusters(collected: Array[(String, ProductDesc)]): Clusters = {
    val builder = ClustersBuilder()
    collected.foreach({ case (id, p) => builder.assignToCluster(p, id)})
    builder.build()
  }
}
