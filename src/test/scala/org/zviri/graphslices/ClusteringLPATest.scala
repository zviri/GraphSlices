package org.zviri.graphslices

import org.scalatest.{FlatSpec, Matchers}

class ClusteringLPATest extends FlatSpec with Matchers {

  "clusterLPA" should "should cluster graph (assuming undirected) by algorithm described in https://arxiv.org/pdf/1103.4550.pdf" in {

    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit),
      Vertex(Seq(5), Unit),
      Vertex(Seq(6), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), Unit),
      Edge(Seq(2), Seq(2), Seq(3), Unit),
      Edge(Seq(3), Seq(3), Seq(1), Unit),
      Edge(Seq(4), Seq(3), Seq(4), Unit),
      Edge(Seq(5), Seq(4), Seq(5), Unit),
      Edge(Seq(6), Seq(5), Seq(6), Unit),
      Edge(Seq(7), Seq(6), Seq(4), Unit)
    ))

    for (_ <- Range(1, 100)) {
      // randomized algorithm, testing multiple times
      val clusterGraph = Algorithms.clusterLPA(graph)

      clusterGraph.vertices.map(v => v.data).distinct.size should be <= 2
      clusterGraph.vertexIndex(1).value.get.data shouldEqual clusterGraph.vertexIndex(2).value.get.data
      clusterGraph.vertexIndex(2).value.get.data shouldEqual clusterGraph.vertexIndex(3).value.get.data
      clusterGraph.vertexIndex(4).value.get.data shouldEqual clusterGraph.vertexIndex(5).value.get.data
      clusterGraph.vertexIndex(5).value.get.data shouldEqual clusterGraph.vertexIndex(6).value.get.data
    }
  }
}
