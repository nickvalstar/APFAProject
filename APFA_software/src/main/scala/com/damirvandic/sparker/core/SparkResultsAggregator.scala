package com.damirvandic.sparker.core

object SparkResultsAggregator {
  def aggregate(x: JobResult, y: JobResult): JobResult = {
    val resX = x.results
    val resY = y.results
    if (x.algorithmName != y.algorithmName) {
      throw new IllegalArgumentException("Job results are not for the same algorithms")
    }
    if (resX.size != resY.size) {
      throw new IllegalArgumentException("Job results are not of the same size!")
    }
    val zipped = resX.zip(resY)
    val aggregated = zipped.map({
      case (b1, b2) => {
        if (b1.bootstrapID != b2.bootstrapID) {
          throw new IllegalArgumentException("Bootstrap IDs do not match with given sequence of BootstrapResults")
        }
        if (b1.testResuslt.f1 > b2.testResuslt.f1) b1 else b2
      }
    })
    JobResult(x.algorithmName, aggregated)
  }
}
