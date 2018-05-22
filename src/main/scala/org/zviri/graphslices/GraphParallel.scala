package org.zviri.graphslices

class GraphParallel[VD, ED] private[graphslices] (_vertices: Seq[Vertex[VD]], _edges: Seq[Edge[ED]], _numDimensions: Int) extends GraphSerial[VD, ED](_vertices, _edges, _numDimensions) {

  override def triplets(): Seq[EdgeTriplet[VD, ED]] = {
    val vertexMap = vertices.map(v => (v.id, v)).toMap
    edges.par.map(e => EdgeTriplet(vertexMap(e.srcId), vertexMap(e.dstId), e)).seq
  }

  override def mapTriplets[ED2](mapFunc: EdgeTriplet[VD, ED] => ED2)
  : Graph[VD, ED2] = {
    val newEdges = triplets().par.map(triplet => Edge(triplet.edge.id, triplet.edge.srcId, triplet.edge.dstId, mapFunc(triplet))).seq

    new GraphParallel(vertices, newEdges, numDimensions)
  }

  override def subgraph(edgePredicate: EdgeTriplet[VD, ED] => Boolean, vertexPredicate: Vertex[VD] => Boolean)
  : Graph[VD, ED] = {
    val newVertices = vertices.filter(vertexPredicate)
    val newEdges = triplets().par.filter(
      triplet => vertexPredicate(triplet.srcVertex) && vertexPredicate(triplet.dstVertex) && edgePredicate(triplet)
    ).map(triplet => triplet.edge).seq
    new GraphParallel(newVertices, newEdges, numDimensions)
  }

  override def aggregateNeighbors[A](mapFunc: (EdgeContext[VD, ED, A]) => Seq[Message[A]], reduceFunc: (A, A) => A): Graph[A, ED] = {
    val vertexMap = vertices.map(v => (v.id, v)).toMap

    val newVertices = edges.par.flatMap {
      edge =>
        mapFunc(new EdgeContext(vertexMap(edge.srcId), vertexMap(edge.dstId), edge))
    }.groupBy(_.vertexId).map(
      group => (group._1, group._2.map(_.message).reduce(reduceFunc))
    ).toVector.map(v => Vertex(v._1, v._2))

    new GraphParallel(newVertices, edges, numDimensions)
  }

  override def outerJoinVertices[D, VD2](other: Seq[(Id, D)])(mapFunc: (Vertex[VD], Option[D]) => VD2)(implicit eq: VD =:= VD2 = null)
  : Graph[VD2, ED] = {
    val otherMap = other.toMap
    val newVertices = vertices.par.map(v => Vertex(v.id, mapFunc(v, otherMap.get(v.id)))).seq
    new GraphParallel(newVertices, edges, numDimensions)
  }

  override def mapDimension[VD2, ED2](mapFunc: Graph[VD, ED] => Graph[VD2, ED2]): Graph[VD2, ED2] = {
    val topDimKeys = vertexIndex.keys
    assert(topDimKeys.toSet == edgeIndex.keys.toSet)

    val (vertices, edges) = topDimKeys.map {
      key =>
        val currVertices = vertexIndex(key).flatten.map {
          case (id, vertex) => (id, Vertex(id, vertex.data))
        }
        val currEdges = edgeIndex(key).flatten.map {
          case (id, edge) => (id, Edge(id, edge.srcId.drop(1), edge.dstId.drop(1), edge.data))
        }
        (key, new GraphSerial(currVertices.map(_._2), currEdges.map(_._2), numDimensions - 1))
    }.par.map {
      case (key, graph) => (key, mapFunc(graph))
    }.map {
      case (key, graph) =>
        val newVertices = graph.vertices.map(v => Vertex(key +: v.id, v.data))
        val newEdges = graph.edges.map(e => Edge(key +: e.id, key +: e.srcId, key +: e.dstId, e.data))
        (newVertices, newEdges)
    }.seq.foldLeft((Seq[Vertex[VD2]](), Seq[Edge[ED2]]())) {
      case ((vs, es), (vc, ec)) => (vs ++ vc, es ++ ec)
    }
    new GraphSerial(vertices, edges, numDimensions)
  }

  override def reverseEdges(): Graph[VD, ED] = {
    new GraphParallel(vertices, edges.map(e => Edge(e.id, e.dstId, e.srcId, e.data)), numDimensions)
  }

  override def updateVertices[VD2](newVertices: Seq[Vertex[VD2]]): Graph[VD2, ED] = new GraphParallel(newVertices, edges, numDimensions)

  override def updateEdges[ED2](newEdges: Seq[Edge[ED2]]): Graph[VD, ED2] = new GraphParallel(vertices, newEdges, numDimensions)
}

object GraphParallel {
  def apply[VD, ED](vertices: Seq[Vertex[VD]], edges: Seq[Edge[ED]]): Graph[VD, ED] = {
    require(vertices.length == vertices.map(_.id).distinct.length, "The list of vertices must contain unique vertices.")
    new GraphParallel[VD, ED](vertices, edges, 1)
  }
}