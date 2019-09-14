package com.damirvandic.sparker.data

class Pair[T](x: T, y: T) {
  val a, b = if (x.hashCode < y.hashCode) (x, y) else (y, x)

  override def equals(other: Any): Boolean = other match {
    case that: Pair[T] =>
      (that canEqual this) &&
        a == that.a &&
        b == that.b
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Pair[T]]

  override def hashCode(): Int = {
    var ret = 0
    ret = 31 * ret + a.hashCode()
    ret = 31 * ret + b.hashCode()
    ret
  }
}
