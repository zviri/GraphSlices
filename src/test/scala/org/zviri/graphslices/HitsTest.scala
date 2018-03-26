package org.zviri.graphslices

import org.scalatest.{FunSuite, Matchers}

class HitsTest extends FunSuite with Matchers {

  val errTol = 0.00001

  //////////////////////// UN-WEIGHTED //////////////////////////////

  test("Simple graph") {
    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), 1.0),
      Edge(Seq(2), Seq(2), Seq(3), 1.0),
      Edge(Seq(3), Seq(3), Seq(1), 1.0),
      Edge(Seq(4), Seq(2), Seq(4), 1.0)
    ))

    val haGraph = Algorithms.hits(graph, numIter = 100)
    assert(haGraph.vertexIndex(1).value.get.data.hub === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(2).value.get.data.hub === 0.9999999925494194 +- errTol)
    assert(haGraph.vertexIndex(3).value.get.data.hub === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(4).value.get.data.hub === 0.0 +- errTol)

    assert(haGraph.vertexIndex(1).value.get.data.authority === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(2).value.get.data.authority === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(3).value.get.data.authority === 0.4999999962747097 +- errTol)
    assert(haGraph.vertexIndex(4).value.get.data.authority === 0.4999999962747097 +- errTol)
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
    )).pushDimension(e => Seq(e. data)).mapEdges(_ => 1.0)

    val haGraph = Algorithms.hits(graph, numIter = 100)
    assert(haGraph.vertexIndex(1)(1).value.get.data.hub === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(1)(2).value.get.data.hub === 0.9999999925494194 +- errTol)
    assert(haGraph.vertexIndex(1)(3).value.get.data.hub === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(1)(4).value.get.data.hub === 0.0 +- errTol)
    assert(haGraph.vertexIndex(1)(1).value.get.data.authority === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(1)(2).value.get.data.authority === 3.7252902707063384e-09 +- errTol)
    assert(haGraph.vertexIndex(1)(3).value.get.data.authority === 0.4999999962747097 +- errTol)
    assert(haGraph.vertexIndex(1)(4).value.get.data.authority === 0.4999999962747097 +- errTol)

    assert(haGraph.vertexIndex(2)(1).value.get.data.hub === 2.48352685947337e-09 +- errTol)
    assert(haGraph.vertexIndex(2)(2).value.get.data.hub === 0.333333332505491 +- errTol)
    assert(haGraph.vertexIndex(2)(3).value.get.data.hub === 0.333333332505491 +- errTol)
    assert(haGraph.vertexIndex(2)(4).value.get.data.hub === 0.333333332505491 +- errTol)
    assert(haGraph.vertexIndex(2)(1).value.get.data.authority === 0.49999999813735485 +- errTol)
    assert(haGraph.vertexIndex(2)(2).value.get.data.authority === 3.7252902845841263e-09 +- errTol)
    assert(haGraph.vertexIndex(2)(3).value.get.data.authority === 0.24999999906867743 +- errTol)
    assert(haGraph.vertexIndex(2)(4).value.get.data.authority === 0.24999999906867743 +- errTol)
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
      Edge(Seq(1), Seq(1), Seq(2), 1.0),
      Edge(Seq(2), Seq(2), Seq(3), 1.0),
      Edge(Seq(3), Seq(1), Seq(4), 1.0),
      Edge(Seq(4), Seq(2), Seq(5), 1.0),
      Edge(Seq(5), Seq(3), Seq(6), 1.0),
      Edge(Seq(6), Seq(4), Seq(5), 1.0),
      Edge(Seq(7), Seq(5), Seq(6), 1.0),
      Edge(Seq(8), Seq(4), Seq(7), 1.0),
      Edge(Seq(9), Seq(5), Seq(8), 1.0),
      Edge(Seq(10), Seq(6), Seq(9), 1.0),
      Edge(Seq(11), Seq(7), Seq(8), 1.0),
      Edge(Seq(12), Seq(8), Seq(9), 1.0)
    ))

    val prGraph = Algorithms.hits(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data.hub === 1.7013564044974136e-09 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.hub === 0.21428571319198517 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.hub === 0.1428571421279901 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.hub === 0.21428571319198517 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.hub === 0.2857142842559802 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.hub === 1.7013564044974136e-09 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.hub === 0.1428571421279901 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.hub === 1.7013564044974136e-09 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.hub === 0.0 +- errTol)

    assert(prGraph.vertexIndex(1).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.authority === 1.4886868526688434e-09 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.authority === 0.12499999925565658 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.authority === 1.4886868526688434e-09 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.authority === 0.24999999851131316 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.authority === 0.24999999851131316 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.authority === 0.12499999925565658 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.authority === 0.24999999851131316 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.authority === 2.977373705337687e-09 +- errTol)
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
      Edge(Seq(1), Seq(2), Seq(1), 1.0),
      Edge(Seq(2), Seq(3), Seq(1), 1.0),
      Edge(Seq(3), Seq(4), Seq(1), 1.0),
      Edge(Seq(4), Seq(5), Seq(1), 1.0),
      Edge(Seq(5), Seq(6), Seq(1), 1.0),
      Edge(Seq(6), Seq(7), Seq(1), 1.0),
      Edge(Seq(7), Seq(8), Seq(1), 1.0),
      Edge(Seq(8), Seq(9), Seq(1), 1.0)
    ))

    val prGraph = Algorithms.hits(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data.hub === 0.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.hub === 0.125 +- errTol)

    assert(prGraph.vertexIndex(1).value.get.data.authority === 1.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.authority === 0.0 +- errTol)
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
      Edge(Seq(1), Seq(1), Seq(2), 1.0),
      Edge(Seq(2), Seq(2), Seq(3), 1.0),
      Edge(Seq(3), Seq(3), Seq(4), 1.0),
      Edge(Seq(4), Seq(4), Seq(5), 1.0),
      Edge(Seq(5), Seq(5), Seq(6), 1.0),
      Edge(Seq(6), Seq(6), Seq(7), 1.0),
      Edge(Seq(7), Seq(7), Seq(8), 1.0),
      Edge(Seq(8), Seq(8), Seq(9), 1.0)
    ))

    val prGraph = Algorithms.hits(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.hub === 0.125 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.hub === 0.0 +- errTol)

    assert(prGraph.vertexIndex(1).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.authority === 0.125 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.authority === 0.125 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.authority === 0.125 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.authority === 0.125 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.authority === 0.125 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.authority === 0.125 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.authority === 0.125 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.authority === 0.125 +- errTol)
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
      Edge(Seq(1), Seq(1), Seq(2), 1.0),
      Edge(Seq(2), Seq(2), Seq(3), 1.0),
      Edge(Seq(3), Seq(3), Seq(4), 1.0),
      Edge(Seq(4), Seq(4), Seq(5), 1.0),
      Edge(Seq(5), Seq(5), Seq(6), 1.0),
      Edge(Seq(6), Seq(6), Seq(7), 1.0),
      Edge(Seq(7), Seq(7), Seq(8), 1.0),
      Edge(Seq(8), Seq(8), Seq(9), 1.0),
      Edge(Seq(9), Seq(9), Seq(1), 1.0)
    ))

    val prGraph = Algorithms.hits(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.hub === 0.1111111111111111 +- errTol)

    assert(prGraph.vertexIndex(1).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.authority === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.authority === 0.1111111111111111 +- errTol)
  }


  /////////////// WEIGHTED //////////////////////////////

  test("Simple graph - weighted") {
    val graph = Graph(Vector(
      Vertex(Seq(1), Unit),
      Vertex(Seq(2), Unit),
      Vertex(Seq(3), Unit),
      Vertex(Seq(4), Unit)
    ), Vector(
      Edge(Seq(1), Seq(1), Seq(2), 1.0),
      Edge(Seq(2), Seq(2), Seq(3), 2.0),
      Edge(Seq(3), Seq(3), Seq(1), 3.0),
      Edge(Seq(4), Seq(2), Seq(4), 4.0)
    ))

    val haGraph = Algorithms.hits(graph, numIter = 100)
    assert(haGraph.vertexIndex(1).value.get.data.hub === 5.960464449200337e-32 +- errTol)
    assert(haGraph.vertexIndex(2).value.get.data.hub === 0.9999999952455494 +- errTol)
    assert(haGraph.vertexIndex(3).value.get.data.hub === 4.7544504819886075e-09 +- errTol)
    assert(haGraph.vertexIndex(4).value.get.data.hub === 0.0 +- errTol)

    assert(haGraph.vertexIndex(1).value.get.data.authority === 5.282722754974404e-09 +- errTol)
    assert(haGraph.vertexIndex(2).value.get.data.authority === 1.9868214820171964e-31 +- errTol)
    assert(haGraph.vertexIndex(3).value.get.data.authority === 0.3333333315724258 +- errTol)
    assert(haGraph.vertexIndex(4).value.get.data.authority === 0.6666666631448516 +- errTol)
  }

  test("Grid graph - weighted") {
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
      Edge(Seq(1), Seq(1), Seq(2), 1.0),
      Edge(Seq(2), Seq(2), Seq(3), 2.0),
      Edge(Seq(3), Seq(1), Seq(4), 3.0),
      Edge(Seq(4), Seq(2), Seq(5), 4.0),
      Edge(Seq(5), Seq(3), Seq(6), 5.0),
      Edge(Seq(6), Seq(4), Seq(5), 6.0),
      Edge(Seq(7), Seq(5), Seq(6), 7.0),
      Edge(Seq(8), Seq(4), Seq(7), 8.0),
      Edge(Seq(9), Seq(5), Seq(8), 9.0),
      Edge(Seq(10), Seq(6), Seq(9), 10.0),
      Edge(Seq(11), Seq(7), Seq(8), 11.0),
      Edge(Seq(12), Seq(8), Seq(9), 12.0)
    ))

    val prGraph = Algorithms.hits(graph, numIter = 1000)

    assert(prGraph.vertexIndex(1).value.get.data.hub === 0.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.hub === 3.929707785034848e-88 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.hub === 6.271612259916052e-09 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.hub === 1.418749199350384e-87 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.hub === 3.634047109736611e-08 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.hub === 0.4545454198650958 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.hub === 3.3684705918702214e-08 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.hub === 0.5454545038381149 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.hub === 0.0 +- errTol)

    assert(prGraph.vertexIndex(1).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.authority === 1.621294006962569e-88 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.authority === 2.0802745410737705e-87 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.authority === 2.7595093428493252e-08 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.authority === 2.3413543195750087e-87 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.authority === 6.736941057977139e-08 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.authority === 0.9999999050354961 +- errTol)
  }

  test("Star graph = weighted") {
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
      Edge(Seq(1), Seq(2), Seq(1), 1.0),
      Edge(Seq(2), Seq(3), Seq(1), 2.0),
      Edge(Seq(3), Seq(4), Seq(1), 3.0),
      Edge(Seq(4), Seq(5), Seq(1), 4.0),
      Edge(Seq(5), Seq(6), Seq(1), 5.0),
      Edge(Seq(6), Seq(7), Seq(1), 6.0),
      Edge(Seq(7), Seq(8), Seq(1), 7.0),
      Edge(Seq(8), Seq(9), Seq(1), 8.0)
    ))

    val prGraph = Algorithms.hits(graph, numIter = 100)

    assert(prGraph.vertexIndex(1).value.get.data.hub === 0.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.hub === 0.027777777777777776 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.hub === 0.05555555555555555 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.hub === 0.08333333333333333 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.hub === 0.1111111111111111 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.hub === 0.1388888888888889 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.hub === 0.16666666666666666 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.hub === 0.19444444444444442 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.hub === 0.2222222222222222 +- errTol)

    assert(prGraph.vertexIndex(1).value.get.data.authority === 1.0 +- errTol)
    assert(prGraph.vertexIndex(2).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(3).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(4).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(5).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(6).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(7).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(8).value.get.data.authority === 0.0 +- errTol)
    assert(prGraph.vertexIndex(9).value.get.data.authority === 0.0 +- errTol)
  }
}
