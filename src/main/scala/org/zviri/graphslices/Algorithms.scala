package org.zviri.graphslices

object Algorithms {

  def pagerank[VD, ED](graph: Graph[VD, ED], resetProb: Double = 0.15, numIter: Int = 100): Graph[Double, Double] = {
    val degrees = graph.outDegree().vertices.map(v => (v.id, v.data))

    var i = 0
    var rankGraph = graph.outerJoinVertices(degrees) {
      (vertex, degree) => degree.getOrElse(0)
    }.mapTriplets(
      triplet => 1.0 / triplet.srcVertex.data
    ).mapVertices[Double](_ => 1.0)

    while (i < numIter) {
      i += 1

      val rankUpdates = rankGraph.aggregateNeighbors[Double](
        (v, e) => v.data * e.data,
        _ + _
      )

      rankGraph = rankGraph.outerJoinVertices(rankUpdates.vertices.map(v => (v.id, v.data))) {
        (vertex, prSum) => resetProb + (1.0 - resetProb) * prSum.getOrElse(0.0)
      }
    }

    def normalizeRec(graph: Graph[Double, Double]): Graph[Double, Double] = {
      if (graph.numDimensions == 1) {
        val prSum = graph.vertices.map(v => v.data).sum
        val numVertices: Double = graph.vertices.size.toDouble
        val correctionFactor = numVertices / prSum
        graph.mapVertices(v =>  (v.data * correctionFactor) / numVertices)
      } else {
        graph.mapDimension(normalizeRec)
      }
    }

    normalizeRec(rankGraph)
  }

  case class HitsScore(hub: Double, authority: Double)
  def hits[VD, ED](graph: Graph[VD, ED], numIter: Int = 100): Graph[HitsScore, Double] = {

    def normalizeRec(graph: Graph[Double, Double]): Graph[Double, Double] = {
      if (graph.numDimensions == 1) {
        val sum = graph.vertices.map(v => v.data).sum
        graph.mapVertices(v =>  v.data / sum)
      } else {
        graph.mapDimension(normalizeRec)
      }
    }

    var i = 0
    var hGraph = graph.mapVertices(_ => 1.0 / graph.vertices.size).mapEdges(_ => 1.0)
    var aGraph = hGraph.reverseEdges()
    while (i < numIter) {
      i += 1

      val hGraphUpdates = hGraph.aggregateNeighbors[Double](
        (v, _) => v.data,
        (a, b) => a + b
      )
      aGraph = aGraph.outerJoinVertices(hGraphUpdates.vertices.map(v => (v.id, v.data))) {
        (vertex, hSum) => hSum.getOrElse(0.0)
      }

      val aGraphUpdates = aGraph.aggregateNeighbors[Double](
        (v, _) => v.data,
        (a, b) => a + b
      )
      hGraph = hGraph.outerJoinVertices(aGraphUpdates.vertices.map(v => (v.id, v.data))) {
        (vertex, aSum) => aSum.getOrElse(0.0)
      }
    }

    hGraph = normalizeRec(hGraph)
    aGraph = normalizeRec(aGraph)

    hGraph.outerJoinVertices(aGraph.vertices.map(v => (v.id, v.data))) {
      (vertex, authority) => HitsScore(vertex.data, authority.getOrElse(0.0))
    }
  }
}
