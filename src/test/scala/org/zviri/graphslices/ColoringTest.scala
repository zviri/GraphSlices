package org.zviri.graphslices

import org.scalatest.{FlatSpec, Matchers}

class ColoringTest extends FlatSpec with Matchers {

  "maxIndependentSet" should "should find maximum independent set in a grid graph using Luby's algorithm" in {

    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit),
      Vertex(Seq(5), Unit),
      Vertex(Seq(6), Unit),
      Vertex(Seq(7), Unit),
      Vertex(Seq(8), Unit),
      Vertex(Seq(9), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), Unit),
      Edge(Seq(2), Seq(2), Seq(3), Unit),
      Edge(Seq(3), Seq(1), Seq(4), Unit),
      Edge(Seq(4), Seq(2), Seq(5), Unit),
      Edge(Seq(5), Seq(3), Seq(6), Unit),
      Edge(Seq(6), Seq(4), Seq(5), Unit),
      Edge(Seq(7), Seq(5), Seq(6), Unit),
      Edge(Seq(8), Seq(4), Seq(7), Unit),
      Edge(Seq(9), Seq(5), Seq(8), Unit),
      Edge(Seq(10), Seq(6), Seq(9), Unit),
      Edge(Seq(11), Seq(7), Seq(8), Unit),
      Edge(Seq(12), Seq(8), Seq(9), Unit)
    ))

    for (_ <- Range(1, 100)) { // randomized algorithm, testing multiple times
      val independentSet = Algorithms.maxIndependentSet(graph)

      independentSet.vertices.map(v => v.data).nonEmpty shouldBe true // non empty

      independentSet.mapTriplets(
        triplet => (triplet.srcVertex.data == triplet.dstVertex.data && triplet.dstVertex.data == 0) | (triplet.srcVertex.data != triplet.dstVertex.data)
      ).edges.forall(e => e.data) shouldBe true // is an independent set
    }

  }

  "maxIndependentSet" should "should find maximum independent set in a loop graph using Luby's algorithm" in {

    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit),
      Vertex(Seq(5), Unit),
      Vertex(Seq(6), Unit),
      Vertex(Seq(7), Unit),
      Vertex(Seq(8), Unit),
      Vertex(Seq(9), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), Unit),
      Edge(Seq(2), Seq(2), Seq(3), Unit),
      Edge(Seq(3), Seq(3), Seq(4), Unit),
      Edge(Seq(4), Seq(4), Seq(5), Unit),
      Edge(Seq(5), Seq(5), Seq(6), Unit),
      Edge(Seq(6), Seq(6), Seq(7), Unit),
      Edge(Seq(7), Seq(7), Seq(8), Unit),
      Edge(Seq(8), Seq(8), Seq(9), Unit),
      Edge(Seq(8), Seq(9), Seq(1), Unit)
    ))

    for (_ <- Range(1, 100)) { // randomized algorithm, testing multiple times
    val independentSet = Algorithms.maxIndependentSet(graph)

      independentSet.vertices.map(v => v.data).nonEmpty shouldBe true // non empty

      independentSet.mapTriplets(
        triplet => (triplet.srcVertex.data == triplet.dstVertex.data && triplet.dstVertex.data == 0) | (triplet.srcVertex.data != triplet.dstVertex.data)
      ).edges.forall(e => e.data) shouldBe true // is an independent set
    }

  }

  "maxIndependentSet" should "should find maximum independent set in a chain graph using Luby's algorithm" in {

    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit),
      Vertex(Seq(5), Unit),
      Vertex(Seq(6), Unit),
      Vertex(Seq(7), Unit),
      Vertex(Seq(8), Unit),
      Vertex(Seq(9), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), Unit),
      Edge(Seq(2), Seq(2), Seq(3), Unit),
      Edge(Seq(3), Seq(3), Seq(4), Unit),
      Edge(Seq(4), Seq(4), Seq(5), Unit),
      Edge(Seq(5), Seq(5), Seq(6), Unit),
      Edge(Seq(6), Seq(6), Seq(7), Unit),
      Edge(Seq(7), Seq(7), Seq(8), Unit),
      Edge(Seq(8), Seq(8), Seq(9), Unit)
    ))

    for (_ <- Range(1, 100)) { // randomized algorithm, testing multiple times
    val independentSet = Algorithms.maxIndependentSet(graph)

      independentSet.vertices.map(v => v.data).nonEmpty shouldBe true // non empty

      independentSet.mapTriplets(
        triplet => (triplet.srcVertex.data == triplet.dstVertex.data && triplet.dstVertex.data == 0) | (triplet.srcVertex.data != triplet.dstVertex.data)
      ).edges.forall(e => e.data) shouldBe true // is an independent set
    }
  }

  "maxIndependentSet" should "should find maximum independent set in a multi-dimensional graph using Luby's algorithm" in {

    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), 1),
      Edge(Seq(2), Seq(2), Seq(3), 1),
      Edge(Seq(3), Seq(3), Seq(1), 1),
      Edge(Seq(4), Seq(2), Seq(4), 1),
      Edge(Seq(6), Seq(1), Seq(2), 2),
      Edge(Seq(7), Seq(2), Seq(3), 2),
      Edge(Seq(8), Seq(3), Seq(1), 2),
      Edge(Seq(9), Seq(2), Seq(4), 2),
      Edge(Seq(5), Seq(4), Seq(1), 2)
    )).pushDimension(e => Seq(e. data))

    for (_ <- Range(1, 100)) { // randomized algorithm, testing multiple times
    val independentSet = Algorithms.maxIndependentSet(graph)

      independentSet.vertices.map(v => v.data).nonEmpty shouldBe true // non empty

      independentSet.mapTriplets(
        triplet => (triplet.srcVertex.data == triplet.dstVertex.data && triplet.dstVertex.data == 0) | (triplet.srcVertex.data != triplet.dstVertex.data)
      ).edges.forall(e => e.data) shouldBe true // is an independent set
    }
  }
}
