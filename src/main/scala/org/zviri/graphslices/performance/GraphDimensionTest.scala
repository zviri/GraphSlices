package org.zviri.graphslices.performance

import org.scalameter.api._
import org.zviri.graphslices.{Edge, Graph, Vertex}

object GraphDimensionTest
  extends Bench.LocalTime {
  val sizes = Gen.range("Complete Graph Size (nodes)")(10, 50, 10)

  val graphs = for {
    size <- sizes
  } yield {
    val nodes = (0 until size).map(id => Vertex(Seq(id.toLong), 1.0))

    val edges = nodes.flatMap(
      n1 => nodes.map(n2 => (n1.id, n2.id))
    ).zipWithIndex.flatMap {
      case ((v1id, v2id), edgeId) => Seq(Edge(Seq(edgeId.toLong), v1id, v2id, 1.0))
    }

    Graph(nodes, edges)
  }

  val graphsWithDimension = for {
    size <- sizes
  } yield {
    val nodes = (0 until size).map(id => Vertex(Seq(id.toLong), 1.0))

    val edges = nodes.flatMap(
      n1 => nodes.map(n2 => (n1.id, n2.id))
    ).zipWithIndex.flatMap {
      case ((v1id, v2id), edgeId) => Seq(Edge(Seq(edgeId.toLong), v1id, v2id, 1.0))
    }

    Graph(nodes, edges).pushDimension(e => (0l to 10l).map(id => (id, e.data)))
  }

  performance of "Graph" in {

    measure method "pushDimension" in {
      using(graphs) in {
        g => g.pushDimension(e => (0l until 10l).map(id => (id, e.data)))
      }
    }

    measure method "popDimension" in {
      using(graphsWithDimension) in {
        g => g.popDimension(
          vertices => vertices.map(_._3),
          edges => edges.map(_._3)
        )
      }
    }

    measure method "mapDimension" in {
      using(graphsWithDimension) in {
        g => g.mapDimension(g => g.mapVertices(v => v.data + 1))
      }
    }
  }
}
