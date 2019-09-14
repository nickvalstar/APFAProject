package com.damirvandic.sparker

import com.damirvandic.sparker.data.Reader
import com.damirvandic.sparker.eval.{Sampler, SamplerImpl}
import org.scalatest._

import scala.util.Random

class SamplerSpec extends FlatSpec with ShouldMatchers {
  "The Sampler" should "correctly sample data sets" in {
    val dataset = Reader.readDataSet("dataset-TVs", "./hdfs/TVs-all-merged.json")
    val sampler: Sampler = new SamplerImpl(123l)
    val n = 100
    val bootstraps = for (i <- 1 to n) yield sampler.sample(dataset)
    for (sample <- bootstraps) {
      sample.train ++ sample.test shouldBe dataset.descriptions
      sample.train intersect sample.test shouldBe empty
      sample.train.size > sample.test.size shouldBe true
    }
  }
}
