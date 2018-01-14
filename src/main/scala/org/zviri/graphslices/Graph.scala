package org.zviri.graphslices

import scala.reflect.ClassTag

class Graph[VD, ED] private(val vertices: Seq[Vertex[VD]], val edges: Seq[Edge[ED]], val _numDimensions: Int) extends Serializable {

  lazy val vertexIndex = MultiIndex(vertices.map(_.id), vertices)
  lazy val edgeIndex = MultiIndex(edges.map(_.id), edges)

  def mapVertices[VD2](map: Vertex[VD] => VD2): Graph[VD2, ED] = {
    new Graph[VD2, ED](vertices.map(vertex => Vertex(vertex.id, map(vertex))), edges, numDimensions)
  }

  def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2] = {
    new Graph[VD, ED2](vertices, edges.map(edge => Edge(edge.id, edge.srcId, edge.dstId, map(edge))), numDimensions)
  }

  def numDimensions: Int = _numDimensions

  def aggregateNeighbors[A](mapFunc: (Vertex[VD], Edge[ED]) => A, reduceFunc: (A, A) => A): Graph[A, ED] = {
    val vertexMap = vertices.map(v => (v.id, v)).toMap

    val newVertices = edges.map(
      edge => (edge.dstId, mapFunc(vertexMap(edge.srcId), edge))
    ).groupBy(_._1).map(
      group => (group._1, group._2.map(_._2).reduce(reduceFunc))
    ).toVector.map(v => Vertex(v._1, v._2))

    new Graph(newVertices, edges, numDimensions)
  }

  def outerJoinVertices[D, VD2](other: Seq[(Id, D)])(mapFunc: (Vertex[VD], Option[D]) => VD2)(implicit eq: VD =:= VD2 = null)
  : Graph[VD2, ED] = {
    val otherMap = other.toMap
    val newVertices = vertices.map(v => Vertex(v.id, mapFunc(v, otherMap.get(v.id))))
    new Graph(newVertices, edges, numDimensions)
  }

  def pushDimension(mapToSubKey: Edge[ED] => Seq[Long]): Graph[VD, ED] = {
    val newEdges = edges.flatMap(
      e => mapToSubKey(e).map(id => Edge(id +: e.id, id +: e.srcId, id +: e.dstId, e.data))
    )

    val vertexMap = vertices.map(v => (v.id, v)).toMap
    val newVertices = newEdges.flatMap(e => Seq(e.srcId, e.dstId)).distinct.map(
      vid => Vertex(vid, vertexMap(vid.drop(1)).data)
    )

    new Graph(newVertices, newEdges, numDimensions + 1)
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

    new Graph(newVertices, newEdges, numDimensions - 1)
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
          case (id, edge) => (id, Edge(id, edge.srcId.drop(1), edge.dstId.drop(1), edge.data)) // TODO:  get rid of this drop appends when editing keys
        }
        (key, new Graph(currVertices.map(_._2), currEdges.map(_._2), numDimensions - 1))
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
    new Graph(vertices, edges, numDimensions)
  }

  def reverseEdges(): Graph[VD, ED] = {
    new Graph(vertices, edges.map(e => Edge(e.id, e.dstId, e.srcId, e.data)), numDimensions)
  }

  def degree(): Graph[Int, ED] = {
    val degrees = aggregateNeighbors[Int]((_, _) => 1, (a, b) => a + b).vertices.map(v => (v.id, v.data))
    this.outerJoinVertices(degrees) {
      (v, d) => d.getOrElse(0)
    }
  }
}

object Graph {
  def apply[VD, ED](vertices: Seq[Vertex[VD]], edges: Seq[Edge[ED]]): Graph[VD, ED] = new Graph(vertices, edges, 1)
}

class Vertex[VD] private(val id: Id, val data: VD)

object Vertex {
  def apply[VD](id: Id, data: VD): Vertex[VD] = new Vertex(id, data)
}

class Edge[ED] private(val id: Id, val srcId: Id, val dstId: Id, val data: ED)

object Edge {
  def apply[ED](id: Id, srcId: Id, dstId: Id, data: ED) = new Edge(id, srcId, dstId, data)
}


