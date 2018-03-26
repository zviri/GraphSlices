package org.zviri.graphslices

import org.scalatest.{FlatSpec, Matchers}

class AlgorithmsTest extends FlatSpec with Matchers {

  val testGraphSimple = Graph(Seq(
    Vertex(Seq(1), 1),
    Vertex(Seq(2), 2),
    Vertex(Seq(3), 3),
    Vertex(Seq(4), 4)
  ), Seq(
    Edge(Seq(1), Seq(1), Seq(2), 1.0),
    Edge(Seq(2), Seq(1), Seq(3), 2.0),
    Edge(Seq(3), Seq(2), Seq(4), 3.0),
    Edge(Seq(4), Seq(3), Seq(4), 4.0)
  ))

  "inDegree" should "return a in-degree graph" in {
    val degreeGraph = Algorithms.inDegree(testGraphSimple)
    degreeGraph.vertexIndex(1).value.get.data shouldEqual 0
    degreeGraph.vertexIndex(2).value.get.data shouldEqual 1
    degreeGraph.vertexIndex(3).value.get.data shouldEqual 1
    degreeGraph.vertexIndex(4).value.get.data shouldEqual 2
  }

  "outDegree" should "return a out-degree graph" in {
    val degreeGraph = Algorithms.outDegree(testGraphSimple)
    degreeGraph.vertexIndex(1).value.get.data shouldEqual 2
    degreeGraph.vertexIndex(2).value.get.data shouldEqual 1
    degreeGraph.vertexIndex(3).value.get.data shouldEqual 1
    degreeGraph.vertexIndex(4).value.get.data shouldEqual 0
  }

  "inDegreeWeighted" should "return a in-degree graph weighted by edge weights" in {
    val degreeGraph = Algorithms.inDegreeWeighted(testGraphSimple)
    degreeGraph.vertexIndex(1).value.get.data shouldEqual 0.0
    degreeGraph.vertexIndex(2).value.get.data shouldEqual 1.0
    degreeGraph.vertexIndex(3).value.get.data shouldEqual 2.0
    degreeGraph.vertexIndex(4).value.get.data shouldEqual 7.0
  }

  "outDegreeWeighted" should "return a out-degree graph weighted by edge weights" in {
    val degreeGraph = Algorithms.outDegreeWeighted(testGraphSimple)
    degreeGraph.vertexIndex(1).value.get.data shouldEqual 3.0
    degreeGraph.vertexIndex(2).value.get.data shouldEqual 3.0
    degreeGraph.vertexIndex(3).value.get.data shouldEqual 4.0
    degreeGraph.vertexIndex(4).value.get.data shouldEqual 0.0
  }
}
