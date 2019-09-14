package com.damirvandic.sparker.util

import java.util.concurrent.atomic.AtomicInteger

object SafeCounter {
  val counter = new AtomicInteger(1)

  def next = counter.getAndIncrement()
}
