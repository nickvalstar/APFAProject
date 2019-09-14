package com.damirvandic.sparker.core

import java.io.PrintWriter

import com.damirvandic.sparker.eval.Results
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.util.Random

object ResultsWriter {

  def clusters(clusters: Clusters): String = {
    clusters.asSet.map(set => set.map(_.title).mkString("\n")).mkString("\n\n")
  }

  def clusters(tuples: Array[(String, ProductDesc)]): String = {
    val ret = tuples.groupBy(_._1).mapValues(arr => arr.map(_._2).map("\t" + _.title).mkString("\n")).map(t => "clusterID = %s\n%s" format(t._1, t._2))
    ret.mkString("\n\n")
  }

}

class ResultsWriter(fs: FileSystem, origPath: String) {
  val logger = LoggerFactory.getLogger(getClass)
  val path = createResultsFolder()

  def write(chunk: Int, seed: Long, results: IndexedSeq[BootstrapResult]): Unit = {
    val outPath = new Path(path + "chunk_" + chunk)
    fs.mkdirs(outPath)
    val w = new Writer(fs, chunk, seed, results, outPath)
    w.writeResults()
    w.writeAggregates()
    w.writeClusters()
    logger.info("Finished writing results")
  }

  def writeSeed(seed: Long): Unit = {

    val writer = new PrintWriter(fs.create(new Path(path + "seed.txt")))
    try {
      writer.write("%d" format seed)
    } finally {
      writer.close()
    }
  }

  def getAbsolutePath(path: Path): String = {
    if (path == null) ""
    else
      path.getParent match {
        case null => ""
        case p: Path => getAbsolutePath(p.getParent) + "/" + p.getName + "/" + path.getName
      }
  }

  private def createResultsFolder() = {
    val newPath = getAbsolutePath(ensureOutpath(fs, new Path(origPath)))
    val base = if (!origPath.startsWith("/") && newPath.startsWith("/")) newPath.drop(1) else newPath
    val trailingSlash = if (!base.endsWith("/")) base + "/" else base
    trailingSlash.replace("//", "/")
  }

  private def ensureOutpath(fs: FileSystem, path: Path, prefix: String = "results") = {
    val outPath = path
    val out = if (fs.exists(outPath)) {
      val ret = new Path(outPath.getParent, "results_" + Random.alphanumeric.take(30).mkString)
      fs.mkdirs(ret)
      logger.warn(s"Path ${outPath.getName} already exists, using path ${ret.getName}")
      ret
    } else {
      fs.mkdirs(outPath)
      outPath
    }
    out
  }

  class Writer(fs: FileSystem, chunk: Int, seed: Long, results: IndexedSeq[BootstrapResult], outputPath: Path, overwrite: Boolean = false) {
    def writeAggregates(): Unit = {
      val res = results.map(b => (b.testResuslt.f1, b.testResuslt.precision, b.testResuslt.recall, b.testResuslt.accuracy, b.testResuslt.specificity))
      val (f1, pr, re, ac, sp) = res.reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3, t1._4 + t2._4, t1._5 + t2._5))
      val writer = new PrintWriter(fs.create(new Path(outputPath, s"average.txt")))
      val n = results.size
      try {
        writer.write("f1 = %.4f\n" format f1 / n)
        writer.write("precision = %.4f\n" format pr / n)
        writer.write("recall = %.4f\n" format re / n)
        writer.write("accuracy = %.4f\n" format ac / n)
        writer.write("specificity = %.4f\n" format sp / n)
      } finally {
        writer.close()
      }
    }

    def writeClusters(): Unit = {
      val f = ensureOutpath(fs, new Path(outputPath, "clusters"), prefix = "clusters")
      for (b <- results) {
        val writerTrain = new PrintWriter(fs.create(new Path(f, s"clusters_train_${b.bootstrapID}.csv")))
        val writerTest = new PrintWriter(fs.create(new Path(f, s"clusters_test_${b.bootstrapID}.csv")))
        try {
          writerTrain.write(b.clustersStrTrain)
          writerTest.write(b.clustersStrTest)
        } finally {
          writerTrain.close()
          writerTest.close()
        }
      }
    }

    def writeResults() {
      val idCol = IndexedSeq(IndexedSeq("\"id\"") ++ results.map(r => r.bootstrapID.toString))
      val algNameCol = IndexedSeq(IndexedSeq("\"algorithm\"") ++ IndexedSeq.fill(results.size)(results.head.algName))
      val varColumns = getVariableColumns(results)
      val durationColumns = getDurationColumns(results)
      val confColumns = getConfColumns(results)
      val trainColumns = getResultsColumns("train", results, _.trainResult)
      val testColumns = getResultsColumns("test", results, _.testResuslt)
      val data = idCol ++ algNameCol ++ durationColumns ++ varColumns ++ confColumns ++ testColumns ++ trainColumns
      val colCount = data.size
      val rowCount = data.head.size
      val writer = new PrintWriter(fs.create(new Path(outputPath, "results.csv")))
      try {
        for (r <- 0 until rowCount) {
          for (c <- 0 until colCount) {
            writer.write(data(c)(r))
            if (c + 1 < colCount) {
              writer.write(";")
            }
          }
          writer.write('\n')
        }
      } finally {
        writer.close()
      }
    }

    private def getResultsColumns(prefix: String, results: IndexedSeq[BootstrapResult], f: BootstrapResult => Results) = {
      val f1Col = IndexedSeq("\"" + prefix + "_f1\"") ++ results.map(r => "%.4f" format f(r).f1)
      val prCol = IndexedSeq("\"" + prefix + "_precision\"") ++ results.map(r => "%.4f" format f(r).precision)
      val reCol = IndexedSeq("\"" + prefix + "_recall\"") ++ results.map(r => "%.4f" format f(r).recall)
      val acCol = IndexedSeq("\"" + prefix + "_accuracy\"") ++ results.map(r => "%.4f" format f(r).accuracy)
      val spCol = IndexedSeq("\"" + prefix + "_specificity\"") ++ results.map(r => "%.4f" format f(r).specificity)
      val tpCol = IndexedSeq("\"" + prefix + "_tp\"") ++ results.map(r => "%d" format f(r).TP)
      val fpCol = IndexedSeq("\"" + prefix + "_fp\"") ++ results.map(r => "%d" format f(r).FP)
      val tnCol = IndexedSeq("\"" + prefix + "_tn\"") ++ results.map(r => "%d" format f(r).TN)
      val fnCol = IndexedSeq("\"" + prefix + "_fn\"") ++ results.map(r => "%d" format f(r).FN)
      IndexedSeq(f1Col, prCol, reCol, acCol, spCol, tpCol, fpCol, tnCol, fnCol)
    }

    private def getConfColumns(results: IndexedSeq[BootstrapResult]) = {
      val confKeys = results.head.algConfig.keySet.toIndexedSeq.sorted
      val cols = confKeys.map(key => IndexedSeq("\"" + key + "\"") ++ results.map(r => r.algConfig(key).toString).toIndexedSeq)
      cols
    }

    private def getDurationColumns(results: IndexedSeq[BootstrapResult]) = {
      val trainDur = IndexedSeq("\"train_duration\"") ++ results.map(r => "%d" format r.trainDur)
      val testDur = IndexedSeq("\"test_duration\"") ++ results.map(r => "%d" format r.testDur)
      IndexedSeq(trainDur, testDur)
    }

    private def getVariableColumns(results: IndexedSeq[BootstrapResult]) = {
      val keys = results.head.trainVariables.keySet.toIndexedSeq.sorted
      val trainVarCols = keys.map(key => IndexedSeq("train_" + key) ++ results.map(r => r.trainVariables(key)))
      val testVarCols = keys.map(key => IndexedSeq("test_" + key) ++ results.map(r => r.testVariables(key)))
      trainVarCols ++ testVarCols
    }
  }

}
