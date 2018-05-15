package org.zviri.graphslices

trait Graph[VD, ED] {

  def vertexIndex: MultiIndex[Vertex[VD]]

  def edgeIndex: MultiIndex[Edge[ED]]

  def mapVertices[VD2](map: Vertex[VD] => VD2): Graph[VD2, ED]

  def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2]

  def triplets(): Seq[EdgeTriplet[VD, ED]]

  def mapTriplets[ED2](mapFunc: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]

  def numDimensions: Int

  def vertices: Seq[Vertex[VD]]

  def edges: Seq[Edge[ED]]

  def subgraph(edgePredicate: EdgeTriplet[VD, ED] => Boolean = _ => true, vertexPredicate: Vertex[VD] => Boolean = _ => true): Graph[VD, ED]

  def aggregateNeighbors[A](mapFunc: (EdgeContext[VD, ED, A]) => Seq[Message[A]], reduceFunc: (A, A) => A): Graph[A, ED]

  def outerJoinVertices[D, VD2](other: Seq[(Id, D)])(mapFunc: (Vertex[VD], Option[D]) => VD2)(implicit eq: VD =:= VD2 = null) : Graph[VD2, ED]

  def pushDimension[ED2](mapToSubKey: Edge[ED] => Seq[(Long, ED2)], keepAllNodes: Boolean = false): Graph[VD, ED2]

  def popDimension[VD2, ED2](reduceFuncVertices: (Seq[(Long, Seq[Long], VD)]) => VD2, reduceFuncEdges: (Seq[(Long, Seq[Long], ED)]) => ED2): Graph[VD2, ED2]

  def mapDimension[VD2, ED2](mapFunc: Graph[VD, ED] => Graph[VD2, ED2]): Graph[VD2, ED2]

  def reverseEdges(): Graph[VD, ED]

  def updateVertices[VD2](vertices: Seq[Vertex[VD2]]): Graph[VD2, ED]

  def updateEdges[ED2](edges: Seq[Edge[ED2]]): Graph[VD, ED2]
}

class Vertex[VD] private(val id: Id, val data: VD)

object Vertex {
  def apply[VD](id: Id, data: VD): Vertex[VD] = new Vertex(id, data)
}

class Edge[ED] private(val id: Id, val srcId: Id, val dstId: Id, val data: ED)

object Edge {
  def apply[ED](id: Id, srcId: Id, dstId: Id, data: ED) = new Edge(id, srcId, dstId, data)
}

case class EdgeTriplet[VD, ED] private(srcVertex: Vertex[VD], dstVertex: Vertex[VD], edge: Edge[ED])

case class Message[A] private(vertexId: Id, message: A)

class EdgeContext[VD, ED, A](val srcVertex: Vertex[VD], val dstVertex: Vertex[VD], val edge: Edge[ED]) {

  def msgToSrc(message: A): Message[A] = {
    Message(srcVertex.id, message)
  }

  def msgToDst(message: A): Message[A] = {
    Message(dstVertex.id, message)
  }
}