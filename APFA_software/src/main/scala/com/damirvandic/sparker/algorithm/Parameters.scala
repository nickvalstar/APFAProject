package com.damirvandic.sparker.algorithm

trait ParameterGrid extends Iterable[Map[String, Any]] {
  def stringRepr: String
}

case class Range(name: String, values: IndexedSeq[Any])

case class ParameterGridImpl(include: IndexedSeq[Range]) extends ParameterGrid {

  override def stringRepr: String = {
    include.map(r => {
      val vals = r.values.mkString(",")
      s"- '${r.name}' => [$vals]"
    }).mkString("\n")
  }

  override def iterator: Iterator[Map[String, Any]] = {
    if (include.isEmpty) {
      // if nothing is specified, then we have 1 parameter combination: empty map
      Iterator(Map.empty[String, Any])
    } else {
      new Iterator[Map[String, Any]] {
        val mappedSeq = include.map(r => r.values.map(v => (r.name, v)))

        val combinations = combine(mappedSeq).filterNot(_.isEmpty)
        // leave out empty param config (in case there are no params)
        val combIt = combinations.iterator

        override def hasNext: Boolean = combIt.hasNext

        override def next(): Map[String, Any] = combIt.next().toMap
      }
    }
  }

  private def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x.view; b <- y) yield a :+ b
    }
}

case class ParameterGridBuilder() {
  var params: Map[String, IndexedSeq[Any]] = Map.empty

  def addRange(paramName: String, values: Array[Any]): Unit = {
    if (params.contains(paramName)) {
      throw new IllegalArgumentException(s"Parameter '$paramName' already added to parameter grid")
    }
    else {
      params += paramName -> {
        val ret = values.toIndexedSeq.sortBy(_.toString)
        ret
      }
    }
  }

  def build(): ParameterGrid = {
    val vals = params.map({ case (name, values) => Range(name, values)}).toIndexedSeq
    new ParameterGridImpl(vals)
  }
}
