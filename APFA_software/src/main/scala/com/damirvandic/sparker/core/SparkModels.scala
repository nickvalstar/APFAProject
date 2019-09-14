package com.damirvandic.sparker.core

import com.damirvandic.sparker.eval.Results

case class BootstrapResult(algName: String,
                           bootstrapID: Int,
                           algConfig: Map[String, Any],
                           trainResult: Results,
                           testResuslt: Results,
                           clustersStrTrain: String,
                           clustersStrTest: String,
                           trainDur: Long,
                           testDur: Long,
                           trainVariables: Map[String, String],
                           testVariables: Map[String, String]) {

  require(trainVariables.keySet == testVariables.keySet)

  override def toString: String = s"BootstrapResult($bootstrapID, $algConfig, trainRes=$trainResult, testRes=$testResuslt)"
}

case class JobResult(algorithmName: String, results: IndexedSeq[BootstrapResult])