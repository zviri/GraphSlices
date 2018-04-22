package org.zviri.graphslices

import org.scalatest.{FunSuite, Matchers}

class EvolvingPageRankTest extends FunSuite with Matchers {

  val errTol = 0.00001

  test("Evolving graph") {

    val MAX_TIME = 4
    val graph = Graph(Seq(
      Vertex(Seq(1), 1),
      Vertex(Seq(2), 1),
      Vertex(Seq(3), 1),
      Vertex(Seq(4), 1),
      Vertex(Seq(5), 1)
    ), Seq(
      Edge(Seq(1), Seq(1), Seq(2), 1),
      Edge(Seq(2), Seq(1), Seq(3), 1),
      Edge(Seq(3), Seq(2), Seq(4), 1),
      Edge(Seq(4), Seq(3), Seq(4), 1),
      Edge(Seq(5), Seq(1), Seq(5), 2),
      Edge(Seq(6), Seq(5), Seq(4), 3),
      Edge(Seq(7), Seq(1), Seq(4), 4)
    )).pushDimension(
      e => (e.data to MAX_TIME).map(t => (t.toLong, 1.0)), keepAllNodes = true
    )

    val prGraph = Algorithms.evolvingPagerank(
      graph,
      timeDecayFunc = (time: Double) => 1.0 / (time * time),
      numIter = 100)
    assert(prGraph.vertexIndex(1).value.get.data(0)._2 === 0.12088244182532489 +- errTol)
    assert(prGraph.vertexIndex(1).value.get.data(1)._2 === 0.12378221569438644 +- errTol)
    assert(prGraph.vertexIndex(1).value.get.data(2)._2 === 0.11327307864559064 +- errTol)
    assert(prGraph.vertexIndex(1).value.get.data(3)._2 === 0.11283096687509123 +- errTol)

    assert(prGraph.vertexIndex(2).value.get.data(0)._2 === 0.17225747960108795 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data(1)._2 === 0.16227884599284678 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data(2)._2 === 0.14676507780676 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data(3)._2 === 0.13998717866136753 +- errTol)

    assert(prGraph.vertexIndex(3).value.get.data(0)._2 === 0.17225747960108795 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data(1)._2 === 0.16227884599284678 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data(2)._2 === 0.14676507780676 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data(3)._2 === 0.13998717866136753 +- errTol)

    assert(prGraph.vertexIndex(4).value.get.data(0)._2 === 0.41372015714717447 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data(1)._2 === 0.3996562538822259 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data(2)._2 === 0.4506255685688855 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data(3)._2 === 0.4694629866041324 +- errTol)

    assert(prGraph.vertexIndex(5).value.get.data(0)._2 === 0.12088244182532489 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data(1)._2 === 0.1520038384376942 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data(2)._2 === 0.14257119717200387 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data(3)._2 === 0.13773168919804132 +- errTol)
  }

  test("Static graph") {

    val MAX_TIME = 4
    val graph = Graph(Seq(
      Vertex(Seq(1), 1),
      Vertex(Seq(2), 1),
      Vertex(Seq(3), 1),
      Vertex(Seq(4), 1),
      Vertex(Seq(5), 1)
    ), Seq(
      Edge(Seq(1), Seq(1), Seq(2), 1),
      Edge(Seq(2), Seq(1), Seq(3), 1),
      Edge(Seq(3), Seq(2), Seq(4), 1),
      Edge(Seq(4), Seq(3), Seq(4), 1),
      Edge(Seq(5), Seq(1), Seq(5), 1),
      Edge(Seq(6), Seq(5), Seq(4), 1),
      Edge(Seq(7), Seq(1), Seq(4), 1)
    )).pushDimension(
      e => (e.data to MAX_TIME).map(t => (t.toLong, 1.0)), keepAllNodes = true
    )

    val prGraph = Algorithms.evolvingPagerank(
      graph,
      timeDecayFunc = (time: Double) => 1.0 / (time * time),
      numIter = 100)
    assert(prGraph.vertexIndex(1).value.get.data(0)._2 === 0.11183336828126092 +- errTol)
    assert(prGraph.vertexIndex(1).value.get.data(1)._2 === 0.11183336828126092 +- errTol)
    assert(prGraph.vertexIndex(1).value.get.data(2)._2 === 0.11183336828126092 +- errTol)
    assert(prGraph.vertexIndex(1).value.get.data(3)._2 === 0.11183336828126092 +- errTol)

    assert(prGraph.vertexIndex(2).value.get.data(0)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data(1)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data(2)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data(3)._2 === 0.13559795904102886 +- errTol)

    assert(prGraph.vertexIndex(3).value.get.data(0)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data(1)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data(2)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data(3)._2 === 0.13559795904102886 +- errTol)

    assert(prGraph.vertexIndex(4).value.get.data(0)._2 === 0.48137275459565243 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data(1)._2 === 0.48137275459565243 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data(2)._2 === 0.48137275459565243 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data(3)._2 === 0.48137275459565243 +- errTol)

    assert(prGraph.vertexIndex(5).value.get.data(0)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data(1)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data(2)._2 === 0.13559795904102886 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data(3)._2 === 0.13559795904102886 +- errTol)
  }

}
