package org.zviri.graphslices


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
}
