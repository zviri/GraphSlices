package org.zviri.graphslices

class GraphSerial[VD, ED] protected (val _vertices: Seq[Vertex[VD]], val _edges: Seq[Edge[ED]], val _numDimensions: Int) extends Graph[VD, ED] {

  lazy val _vertexIndex = RecursiveMultiIndex(vertices.map(_.id), vertices)
  lazy val _edgeIndex = RecursiveMultiIndex(edges.map(_.id), edges)

  def vertexIndex: MultiIndex[Vertex[VD]] = _vertexIndex

  def edgeIndex: MultiIndex[Edge[ED]] = _edgeIndex

  def mapVertices[VD2](map: Vertex[VD] => VD2): Graph[VD2, ED] = {
    new GraphSerial[VD2, ED](vertices.map(vertex => Vertex(vertex.id, map(vertex))), edges, numDimensions)
  }

  def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2] = {
    new GraphSerial[VD, ED2](vertices, edges.map(edge => Edge(edge.id, edge.srcId, edge.dstId, map(edge))), numDimensions)
  }

  def triplets(): Seq[EdgeTriplet[VD, ED]] = {
    val vertexMap = vertices.map(v => (v.id, v)).toMap
    edges.map(e => EdgeTriplet(vertexMap(e.srcId), vertexMap(e.dstId), e))
  }

  def mapTriplets[ED2](mapFunc: EdgeTriplet[VD, ED] => ED2)
  : Graph[VD, ED2] = {
    val newEdges = triplets().map(triplet => Edge(triplet.edge.id, triplet.edge.srcId, triplet.edge.dstId, mapFunc(triplet)))

    new GraphSerial(vertices, newEdges, numDimensions)
  }

  def numDimensions: Int = _numDimensions

  def vertices: Seq[Vertex[VD]] = _vertices

  def edges: Seq[Edge[ED]] = _edges

  def subgraph(edgePredicate: EdgeTriplet[VD, ED] => Boolean, vertexPredicate: Vertex[VD] => Boolean)
  : Graph[VD, ED] = {
    val newVertices = vertices.filter(vertexPredicate)
    val newEdges = triplets().filter(
      triplet => vertexPredicate(triplet.srcVertex) && vertexPredicate(triplet.dstVertex) && edgePredicate(triplet)
    ).map(triplet => triplet.edge)
    new GraphSerial(newVertices, newEdges, numDimensions)
  }

  def aggregateNeighbors[A](mapFunc: (EdgeContext[VD, ED, A]) => Seq[Message[A]], reduceFunc: (A, A) => A): Graph[A, ED] = {
    val vertexMap = vertices.map(v => (v.id, v)).toMap

    val newVertices = edges.flatMap {
      edge =>
        mapFunc(new EdgeContext(vertexMap(edge.srcId), vertexMap(edge.dstId), edge))
    }.groupBy(_.vertexId).map(
      group => (group._1, group._2.map(_.message).reduce(reduceFunc))
    ).toVector.map(v => Vertex(v._1, v._2))

    new GraphSerial(newVertices, edges, numDimensions)
  }

  def outerJoinVertices[D, VD2](other: Seq[(Id, D)])(mapFunc: (Vertex[VD], Option[D]) => VD2)(implicit eq: VD =:= VD2 = null)
  : Graph[VD2, ED] = {
    val otherMap = other.toMap
    val newVertices = vertices.map(v => Vertex(v.id, mapFunc(v, otherMap.get(v.id))))
    new GraphSerial(newVertices, edges, numDimensions)
  }

  def pushDimension[ED2](mapToSubKey: Edge[ED] => Seq[(Long, ED2)], keepAllNodes: Boolean = false): Graph[VD, ED2] = {
    val newEdges = edges.flatMap(
      e => mapToSubKey(e).map {
        case (id, data) => Edge(id +: e.id, id +: e.srcId, id +: e.dstId, data)
      }
    )

    val vertexMap = vertices.map(v => (v.id, v)).toMap
    val newVertices = keepAllNodes match {
      case true => newEdges.flatMap(
        e => Seq(e.srcId.head, e.dstId.head)
      ).distinct.flatMap(
        newId => vertices.map(v => Vertex(newId +: v.id, v.data))
      )
      case _ => newEdges.flatMap(
        e => Seq(e.srcId, e.dstId)
      ).distinct.map(
        vid => Vertex(vid, vertexMap(vid.drop(1)).data)
      )
    }

    new GraphSerial(newVertices, newEdges, numDimensions + 1)
  }

  def popDimension[VD2, ED2](reduceFuncVertices: (Seq[(Long, Seq[Long], VD)]) => VD2,
                             reduceFuncEdges: (Seq[(Long, Seq[Long], ED)]) => ED2): Graph[VD2, ED2] = {
    if (numDimensions <= 1) throw new IllegalStateException("No dimension to pop")

    val newVertices = vertexIndex.flatten.groupBy(_._1.drop(1)).map {
      case (subKey, currVertices) => Vertex(subKey, reduceFuncVertices(currVertices.map {
        case (id, vertex) => (id.head, id.drop(1), vertex.data)
      }))
    }.toSeq

    val newEdges = edgeIndex.flatten.groupBy(_._1.drop(1)).map {
      case (subKey, currEdges) =>
        val edgeForId = currEdges.head._2
        Edge(subKey, edgeForId.srcId.drop(1), edgeForId.dstId.drop(1), reduceFuncEdges(currEdges.map {
          case (id, edge) => (id.head, id.drop(1), edge.data)
        }))
    }.toSeq

    new GraphSerial(newVertices, newEdges, numDimensions - 1)
  }

  def mapDimension[VD2, ED2](mapFunc: Graph[VD, ED] => Graph[VD2, ED2]): Graph[VD2, ED2] = {
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
    }.map {
      case (key, graph) => (key, mapFunc(graph))
    }.map {
      case (key, graph) =>
        val newVertices = graph.vertices.map(v => Vertex(key +: v.id, v.data))
        val newEdges = graph.edges.map(e => Edge(key +: e.id, key +: e.srcId, key +: e.dstId, e.data))
        (newVertices, newEdges)
    }.foldLeft((Seq[Vertex[VD2]](), Seq[Edge[ED2]]())) {
      case ((vs, es), (vc, ec)) => (vs ++ vc, es ++ ec)
    }
    new GraphSerial(vertices, edges, numDimensions)
  }

  def reverseEdges(): Graph[VD, ED] = {
    new GraphSerial(vertices, edges.map(e => Edge(e.id, e.dstId, e.srcId, e.data)), numDimensions)
  }

  def updateVertices[VD2](newVertices: Seq[Vertex[VD2]]): Graph[VD2, ED] = new GraphSerial(newVertices, edges, numDimensions)

  def updateEdges[ED2](newEdges: Seq[Edge[ED2]]): Graph[VD, ED2] = new GraphSerial(vertices, newEdges, numDimensions)

  def seq: Graph[VD, ED] = new GraphSerial[VD, ED](vertices, edges, numDimensions)

  def par: Graph[VD, ED] = new GraphParallel[VD, ED](vertices, edges, numDimensions)
}

object GraphSerial {
  def apply[VD, ED](vertices: Seq[Vertex[VD]], edges: Seq[Edge[ED]]): Graph[VD, ED] = {
    require(vertices.length == vertices.map(_.id).distinct.length, "The list of vertices must contain unique vertices.")
    new GraphSerial[VD, ED](vertices, edges, 1)
  }
}