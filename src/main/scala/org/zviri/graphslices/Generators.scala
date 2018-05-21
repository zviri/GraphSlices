package org.zviri.graphslices

import scala.collection.mutable
import scala.util.Random


object Generators {
  def completeGraph(numVertices: Long, bidirectionalEdges: Boolean = true): Graph[Unit.type, Unit.type] = {
    val vertices = (0l until numVertices).map(id => Vertex(Seq(id), Unit))

    val edgesDirection1 = vertices.flatMap(
      n1 => vertices.map(n2 => (n1.id, n2.id))
    ).filter {
      case (v1id, v2id) => v1id != v2id
    }.zipWithIndex.map{
      case ((v1id, v2id), edgeId) => Edge(Seq(edgeId.toLong), v1id, v2id, Unit)
    }

    val edges = if (!bidirectionalEdges) edgesDirection1 else {
      val numEdges = edgesDirection1.size
      edgesDirection1.flatMap(e => Seq(e, Edge(Seq(e.id.head + numEdges + 1), e.dstId, e.srcId, Unit)))
    }

    GraphSerial(vertices, edges)
  }

  def barabasiAlbertGraph(n: Int, m: Int): Graph[Unit.type, Unit.type] = {
    require(m > 1 && n >= 1 && m < n, s"Barabási–Albert network must have m >= 1 and m < n, m = $m, n = $n")

    def _random_subset[T](seq: Seq[T], m: Int): Seq[T] = {
      val subset = mutable.Set[T]()
      while (subset.size < m) {
        val idx = Random.nextInt(seq.size)
        subset.add(seq(idx))
      }
      subset.toSeq
    }

    var targets: Seq[Int] = (0 until m).toVector

    val vertices: Seq[Int] = (0 until n).toVector
    var edges = mutable.ListBuffer[(Int, Int)]()

    val nodesAccum = mutable.ListBuffer[Int]()
    var source = m
    while (source < n) {
      edges ++= targets.map(v => (source, v))
      nodesAccum ++= targets
      nodesAccum ++= targets.map(v => source)

      targets = _random_subset(nodesAccum, m)
      source += 1
    }

    edges = edges.flatMap(e => Seq(e, e.swap))

    GraphSerial(
      vertices.map(id => Vertex(Seq(id), Unit)),
      edges.zipWithIndex.map { case ((v1, v2), idx) => Edge(Seq(idx), Seq(v1), Seq(v2), Unit)}
    )
  }
}
