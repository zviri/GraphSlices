package org.zviri.graphslices.performance

import org.scalameter.api._
import org.zviri.graphslices.{Edge, GraphParallel, Vertex}

object GraphParallelOperationsTest extends CustomPerfTest("GraphParallelOperationsTest_") {
    val sizes = Gen.range("Complete Graph Size (nodes)")(100, 1500, 100)

    val graphs = for {
      size <- sizes
    } yield {
      val nodes = (0 until size).map(id => Vertex(Seq(id.toLong), 1.0))

      val edges = nodes.flatMap(
        n1 => nodes.map(n2 => (n1.id, n2.id))
      ).zipWithIndex.flatMap{
        case ((v1id, v2id), edgeId) =>
          Seq(Edge(Seq(edgeId.toLong), v1id, v2id, 1.0), Edge(Seq(edgeId.toLong), v2id, v1id, 1.0))
      }

      GraphParallel(nodes, edges)
    }

    performance of "GraphParallel" in {

      measure method "mapTriplets" in {
        using(graphs) in {
          g => g.mapTriplets(triplet => triplet.edge.data + 1)
        }
      }

      measure method "subgraph" in {
        using(graphs) in {
          g =>
            val numVertices = g.vertices.size
            g.subgraph(
              edgeTriplet => edgeTriplet.edge.id.head % 2 == 0,
              vertex => vertex.id.head < numVertices / 2
            )
        }
      }

      measure method "aggregateNeighbors" in {
        using(graphs) in {
          g => g.aggregateNeighbors[Double](
            edgeCtx => Seq(edgeCtx.msgToDst(edgeCtx.srcVertex.data)),
            _ + _
          )
        }
      }
    }
  }
