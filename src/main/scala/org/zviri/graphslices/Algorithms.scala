package org.zviri.graphslices

object Algorithms {

  def pagerank[VD, ED](graph: Graph[VD, ED], resetProb: Double = 0.15, numIter: Int = 100): Graph[Double, Double] = {
    val degrees = graph.reverseEdges().degree()
    val degMap = degrees.vertices.map(v => (v.id, v.data)).toMap[Id, Int]

    var i = 0
    var rankGraph = graph.mapVertices[Double](_ => 1.0).mapEdges(_ => 1.0)
    while (i < numIter) {
      i += 1

      val rankUpdates = rankGraph.aggregateNeighbors[Double](
        (v, e) => v.data / degMap(v.id),
        (a, b) => a + b
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
}
