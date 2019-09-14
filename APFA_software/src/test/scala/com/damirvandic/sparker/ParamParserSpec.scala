package com.damirvandic.sparker

import com.damirvandic.sparker.algorithm.{ParameterGrid, ParameterGridBuilder}
import com.damirvandic.sparker.core.ParamParser
import org.scalatest.{FlatSpec, MustMatchers}

class ParamParserSpec extends FlatSpec with MustMatchers {

  def get(args: String*): Seq[(String, String)] = {
    args.map(i => {
      val split = i.split("=")
      (split(0).trim, split(1).trim)
    })
  }

  "The ParamParser" must "correctly parse parameters" in {
    val spec = get(
      "a=i 0 1 2 3",
      "b=ri 0 10 2",
      "c=d 0 1 2 3",
      "d=rd 0 5 0.5",
      "e=b true false",
      "f=s YES NO"
    )
    val grid = ParamParser.getParams(spec)
    grid.stringRepr.trim mustBe
      """
        |- 'e' => [false,true]
        |- 'f' => [NO,YES]
        |- 'a' => [0,1,2,3]
        |- 'b' => [0,10,2,4,6,8]
        |- 'c' => [0.0,1.0,2.0,3.0]
        |- 'd' => [0.0,0.5,1.0,1.5,2.0,2.5,3.0,3.5,4.0,4.5,5.0]
      """.stripMargin.drop(1).trim
  }

}
