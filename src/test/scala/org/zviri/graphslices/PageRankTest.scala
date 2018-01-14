package org.zviri.graphslices

import org.scalatest.{FlatSpec, FunSuite, Matchers}
import org.scalatest.Assertions._
import org.scalactic.TripleEquals._

class PageRankTest extends FunSuite with Matchers {

  val errTol = 0.00001

  test("Simple graph") {
    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), 1),
      Edge(Seq(2), Seq(2), Seq(3), 1),
      Edge(Seq(3), Seq(3), Seq(1), 1),
      Edge(Seq(4), Seq(2), Seq(4), 1)
    ))

    val prGraph = Algorithms.pagerank(graph, numIter = 100)
    assert(prGraph.vertexIndex(1).value.get.data === 0.26462302680270133 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data === 0.3078526047028215 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data === 0.21376218424723858 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data === 0.21376218424723858 +- errTol)
  }

  test("Simple graph with dimension") {
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

    val prGraph = Algorithms.pagerank(graph, numIter = 100)

    assert(prGraph.vertexIndex(1)(1).value.get.data === 0.26462302680270133 +- errTol)
    assert(prGraph.vertexIndex(1)(2).value.get.data === 0.3078526047028215 +- errTol)
    assert(prGraph.vertexIndex(1)(3).value.get.data === 0.21376218424723858 +- errTol)
    assert(prGraph.vertexIndex(1)(4).value.get.data === 0.21376218424723858 +- errTol)

    assert(prGraph.vertexIndex(2)(1).value.get.data === 0.33260554622228633 +- errTol)
    assert(prGraph.vertexIndex(2)(2).value.get.data === 0.3202132182582236 +- errTol)
    assert(prGraph.vertexIndex(2)(3).value.get.data === 0.17359061775974502 +- errTol)
    assert(prGraph.vertexIndex(2)(4).value.get.data === 0.17359061775974502 +- errTol)
  }

  test("Grid graph") {
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

    val prGraph = Algorithms.pagerank(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data === 0.04444540879570262 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data === 0.06333477584223254 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data === 0.07136292349194726 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data === 0.06333477584223254 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data === 0.09828043818819188 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data === 0.14687395204183107 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data === 0.07136292349194726 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data === 0.14687395204183107 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data === 0.2941308502640836 +- errTol)
  }

  test("Star graph") {
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
      Edge(Seq(1), Seq(2), Seq(1), Unit),
      Edge(Seq(2), Seq(3), Seq(1), Unit),
      Edge(Seq(3), Seq(4), Seq(1), Unit),
      Edge(Seq(4), Seq(5), Seq(1), Unit),
      Edge(Seq(5), Seq(6), Seq(1), Unit),
      Edge(Seq(6), Seq(7), Seq(1), Unit),
      Edge(Seq(7), Seq(8), Seq(1), Unit),
      Edge(Seq(8), Seq(9), Seq(1), Unit)
    ))

    val prGraph = Algorithms.pagerank(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data === 0.4936692017697505 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data === 0.06329134977878122 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data === 0.06329134977878122 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data === 0.06329134977878122 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data === 0.06329134977878122 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data === 0.06329134977878122 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data === 0.06329134977878122 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data === 0.06329134977878122 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data === 0.06329134977878122 +- errTol)
  }

  test("Chain graph") {
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

    val prGraph = Algorithms.pagerank(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data === 0.0322868281045965 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data === 0.05973025376201947 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data === 0.08305735726771175 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data === 0.10288582782139338 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data === 0.1197404541872224 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data === 0.1340671264578046 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data === 0.14624472471479055 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data === 0.15659521364930995 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data === 0.16539221403515136 +- errTol)
  }

  test("Loop graph") {
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

    val prGraph = Algorithms.pagerank(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data === 0.1111111111111111 +- errTol)
  }
}
