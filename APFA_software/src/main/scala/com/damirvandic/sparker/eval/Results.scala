package com.damirvandic.sparker.eval

trait Results extends Ordered[Results] {
  val precision: Double
  val recall: Double
  val specificity: Double
  val accuracy: Double
  val f1: Double
  val TP: Int
  val FP: Int
  val TN: Int
  val FN: Int
}

case class ResultsImpl(TP: Int,
                       FP: Int,
                       TN: Int,
                       FN: Int) extends Results {

  val precision = check(TP / (TP + FP).toDouble)
  val recall = check(TP / (TP + FN).toDouble)
  val specificity = check(TN / (TN + FP).toDouble)
  val accuracy = check((TP + TN) / (TP + FP + TN + FN).toDouble)
  val f1 = check(2 * ((precision * recall) / (precision + recall)))

  def check(d: Double): Double = if (java.lang.Double.isNaN(d)) 0.0 else d

  def shortRepr: String = f"F1:$f1%.3f"

  override def compare(that: Results): Int = that.f1.compareTo(this.f1)

  override def toString: String = f"Results(f1=$f1%.3f)"
}
