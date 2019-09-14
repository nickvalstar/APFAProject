package com.damirvandic.sparker

import com.damirvandic.sparker.algorithm.ParameterGridBuilder
import org.scalatest.{FlatSpec, MustMatchers}

class ParameterGridSpec extends FlatSpec with MustMatchers {

  "The ParameterGridBuilder" must "correctly include ranges" in {
    val builder = ParameterGridBuilder()
    builder.addRange("paramA", arrayRange(0, .6, .3))
    builder.addRange("paramB", arrayRange(0, .6, .3))
    val grid = builder.build()
    grid.size mustBe 9 // 3 x 3 (i.e., [0.0, 0.3, 0.6] x [0.0, 0.3, 0.6])
    val gridSet = grid.toSet
    checkGrid(gridSet)
  }

  it must "not be dependant on the order of values" in {
    def build(a: Any, b: Any, c: Any, d: Any) = {
      val builder = ParameterGridBuilder()
      builder.addRange("paramA", Array(a, b))
      builder.addRange("paramB", Array(c, d))
      builder.build()
    }

    build(1.0, 2.0, "A", "B").toSeq mustBe build(2.0, 1.0, "A", "B").toSeq
    build(1.0, 2.0, "A", "B").toSeq mustBe build(1.0, 2.0, "B", "A").toSeq
    build(1.0, 2.0, "A", "B").toSeq mustBe build(2.0, 1.0, "B", "A").toSeq
  }

  private def checkGrid(gridSet: Set[Map[String, Any]]): Unit = {
    gridSet contains Map("paramA" -> 0.0, "paramB" -> 0.0) mustBe true
    gridSet contains Map("paramA" -> 0.0, "paramB" -> 0.3) mustBe true
    gridSet contains Map("paramA" -> 0.0, "paramB" -> 0.6) mustBe true
    gridSet contains Map("paramA" -> 0.3, "paramB" -> 0.0) mustBe true
    gridSet contains Map("paramA" -> 0.3, "paramB" -> 0.3) mustBe true
    gridSet contains Map("paramA" -> 0.3, "paramB" -> 0.6) mustBe true
    gridSet contains Map("paramA" -> 0.6, "paramB" -> 0.0) mustBe true
    gridSet contains Map("paramA" -> 0.6, "paramB" -> 0.3) mustBe true
    gridSet contains Map("paramA" -> 0.6, "paramB" -> 0.6) mustBe true
  }

  private def arrayRange(start: Double, end: Double, stepSize: Double): Array[Any] = Range.Double.inclusive(start, end, stepSize).toArray
}
