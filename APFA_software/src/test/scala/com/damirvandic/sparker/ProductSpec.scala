package com.damirvandic.sparker

import com.damirvandic.sparker.core.ProductDesc
import org.scalatest.{FlatSpec, ShouldMatchers}

class ProductSpec extends FlatSpec with ShouldMatchers {

  "A ProductDescription" should "correctly extract the host from the url" in {
    ProductDesc.extractShop("http://google.com") should be("google.com")
    ProductDesc.extractShop("google.com") should be("google.com")
    ProductDesc.extractShop("www.google.com") should be("google.com")
    ProductDesc.extractShop("http://www.google.com") should be("google.com")
    ProductDesc.extractShop("http://www.test.google.com") should be("google.com")
    ProductDesc.extractShop("bla") should be("bla")
  }

}

// TODO test ordering of result object (e.g., DefaultEvalResult)
// TODO test whether max is chosen in training phase