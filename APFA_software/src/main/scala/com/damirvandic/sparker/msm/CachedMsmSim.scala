package com.damirvandic.sparker.msm

import java.io.ObjectInputStream

import com.damirvandic.sparker.core.{ProductDesc, ProductSimilarity}
import com.damirvandic.sparker.util.IntPair
import gnu.trove.map.hash.TObjectFloatHashMap
import org.apache.hadoop.fs.{FileSystem, Path}

case class CachedMsmSim(fs: FileSystem, basePath: String, prefix: String, conf: Map[String, Any]) extends ProductSimilarity {
  val cache: TObjectFloatHashMap[IntPair] = readCache()

  override def computeSim(a: ProductDesc, b: ProductDesc): Double = {
    val key = new IntPair(a.ID, b.ID)
    if (cache.containsKey(key)) {
      cache.get(key)
    } else {
      throw new IllegalArgumentException(s"No sim for entities ${a.ID} and ${b.ID}")
    }
  }

  private def readCache(): TObjectFloatHashMap[IntPair] = {
    val key = CacheConfKey.get(conf)
    val file = prefix + key + ".bin"
    val baseWithSlash = if (basePath.endsWith("/")) basePath else basePath + "/"
    val in = fs.open(new Path(baseWithSlash + file))
    val objReader = new ObjectInputStream(in)
    objReader.readObject().asInstanceOf[TObjectFloatHashMap[IntPair]]
  }
}
