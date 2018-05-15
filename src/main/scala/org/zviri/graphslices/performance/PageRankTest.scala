package org.zviri.graphslices.performance

import org.scalameter.api._
import org.zviri.graphslices._

object PageRankTest
  extends Bench.LocalTime {
    val sizes = Gen.range("Complete Graph Size (nodes)")(100, 800, 10)

    val graphsSerial = for {
      size <- sizes
    } yield {
      val nodes = (0 until size).map(id => Vertex(Seq(id.toLong), 1.0))

      val edges = nodes.flatMap(
        n1 => nodes.map(n2 => (n1.id, n2.id))
      ).zipWithIndex.flatMap{
        case ((v1id, v2id), edgeId) =>
          Seq(Edge(Seq(edgeId.toLong), v1id, v2id, 1.0), Edge(Seq(edgeId.toLong), v2id, v1id, 1.0))
      }

      GraphSerial(nodes, edges)
    }

    performance of "SerialGraph" in {
      measure method  "pagerank" in {
        using(graphsSerial) config (
//          exec.minWarmupRuns -> 1,
          exec.benchRuns -> 1,
          exec.independentSamples -> 1,
          exec.maxWarmupRuns -> 1,
          exec. warmupCovThreshold -> 0.0
          ) in {
              print("done")
          g => Algorithms.pagerank(g.mapVertices(v => 1.0), numIter = 100)
        }
      }
    }

  val graphsParallel = for {
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

  performance of "ParallelGraph" in {
    measure method "pagerank" in {
      using(graphsParallel) config (
        exec.minWarmupRuns -> 1,
        exec.benchRuns -> 1,
        exec.independentSamples -> 1,
        exec.maxWarmupRuns -> 1
        ) in {
        g => Algorithms.pagerank(g, numIter = 100)
      }
    }
  }
  }
