package com.damirvandic.sparker.util

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

object DurationHelper {
  def time[T](f: Duration => Unit)(body: => T): T = {
    val start = System.nanoTime()
    val ret = body
    val end = System.nanoTime()
    val dur = Duration(end - start, TimeUnit.NANOSECONDS)
    f(dur)
    ret
  }

  def timeMillis[T](f: Long => Unit)(body: => T): T = {
    val start = System.nanoTime()
    val ret = body
    val end = System.nanoTime()
    val dur = Duration(end - start, TimeUnit.NANOSECONDS)
    f(dur.toMillis)
    ret
  }

}
