package org.zviri.graphslices

import org.scalatest.{FlatSpec, Matchers}

class GraphTest extends FlatSpec with Matchers {

  val testGraphSimple = Graph(Seq(
    Vertex(Seq(1), 1),
    Vertex(Seq(2), 2),
    Vertex(Seq(3), 3),
    Vertex(Seq(4), 4)
  ), Seq(
    Edge(Seq(1), Seq(1), Seq(2), 1),
    Edge(Seq(2), Seq(1), Seq(3), 2),
    Edge(Seq(3), Seq(2), Seq(4), 3),
    Edge(Seq(4), Seq(3), Seq(4), 4)
  ))

  "mapVertices" should "transformed vertex data" in {
    val graphTransformed = testGraphSimple.mapVertices(v => v.data + 1.0)
    graphTransformed.vertexIndex(1).value.get.data shouldEqual 2.0
    graphTransformed.vertexIndex(2).value.get.data shouldEqual 3.0
    graphTransformed.vertexIndex(3).value.get.data shouldEqual 4.0
    graphTransformed.vertexIndex(4).value.get.data shouldEqual 5.0
  }

  "mapEdges" should "transformed edge data" in {
    val graphTransformed = testGraphSimple.mapEdges(e => e.data + 1.0)
    graphTransformed.edgeIndex(1).value.get.data shouldEqual 2.0
    graphTransformed.edgeIndex(2).value.get.data shouldEqual 3.0
    graphTransformed.edgeIndex(3).value.get.data shouldEqual 4.0
    graphTransformed.edgeIndex(4).value.get.data shouldEqual 5.0
  }

  "aggregateNeighbours" should "send message via mapFunc over each edge and aggregate incoming edges via reduceFunc" in {
    val graphTransformed = testGraphSimple.aggregateNeighbors[Double]((v, e) => (v.data * e.data).toDouble, (m1, m2) => m1 + m2)

    a[NoSuchElementException] should be thrownBy {
      graphTransformed.vertexIndex(1)
    }

    graphTransformed.vertexIndex(2).value.get.data shouldEqual 1.0
    graphTransformed.vertexIndex(3).value.get.data shouldEqual 2.0
    graphTransformed.vertexIndex(4).value.get.data shouldEqual 18.0
  }

  "outerJoinVertices" should "" in {
    val data = Map(Seq(1l) -> 10l, Seq(2l) -> 20l).toSeq

    val graphTransformed = testGraphSimple.outerJoinVertices(data) {
      (v, d) => d.getOrElse(v.data)
    }

    graphTransformed.vertexIndex(1).value.get.data shouldEqual 10
    graphTransformed.vertexIndex(2).value.get.data shouldEqual 20
    graphTransformed.vertexIndex(3).value.get.data shouldEqual 3
    graphTransformed.vertexIndex(4).value.get.data shouldEqual 4
  }

  "pushDimension" should "should add a new dimension to the graph and prepend the new subkey to existing ids" in {
    val graphTransformed = testGraphSimple.pushDimension(e => (1 to 2).map(_.toLong))

    graphTransformed.vertexIndex(1)(1).value.get.data shouldEqual 1
    graphTransformed.vertexIndex(1)(2).value.get.data shouldEqual 2
    graphTransformed.vertexIndex(1)(3).value.get.data shouldEqual 3
    graphTransformed.vertexIndex(1)(4).value.get.data shouldEqual 4

    graphTransformed.vertexIndex(2)(1).value.get.data shouldEqual 1
    graphTransformed.vertexIndex(2)(2).value.get.data shouldEqual 2
    graphTransformed.vertexIndex(2)(3).value.get.data shouldEqual 3
    graphTransformed.vertexIndex(2)(4).value.get.data shouldEqual 4


    graphTransformed.edgeIndex(1)(1).value.get.data shouldEqual 1
    graphTransformed.edgeIndex(1)(2).value.get.data shouldEqual 2
    graphTransformed.edgeIndex(1)(3).value.get.data shouldEqual 3
    graphTransformed.edgeIndex(1)(4).value.get.data shouldEqual 4

    graphTransformed.edgeIndex(2)(1).value.get.data shouldEqual 1
    graphTransformed.edgeIndex(2)(2).value.get.data shouldEqual 2
    graphTransformed.edgeIndex(2)(3).value.get.data shouldEqual 3
    graphTransformed.edgeIndex(2)(4).value.get.data shouldEqual 4

  }

  "popDimension" should "should remove on dimension from the graph and aggregate nodes/edges across that dimension" in {
    val graph = Graph[Int, Int](Seq(
      Vertex(Seq(1), 1),
      Vertex(Seq(2), 2),
      Vertex(Seq(3), 3)
    ), Seq(
      Edge(Seq(1), Seq(1), Seq(2), 11),
      Edge(Seq(2), Seq(2), Seq(3), 12),
      Edge(Seq(3), Seq(3), Seq(1), 13)
    )).pushDimension(e => (1 to 2).map(_.toLong))

    val graphTransformed = graph.popDimension(
      vertices => vertices.map(_._3),
      edges => edges.map(_._3)
    )

    graphTransformed.vertexIndex(1).value.get.data should contain theSameElementsAs Seq(1, 1)
    graphTransformed.vertexIndex(2).value.get.data should contain theSameElementsAs Seq(2, 2)
    graphTransformed.vertexIndex(3).value.get.data should contain theSameElementsAs Seq(3, 3)

    graphTransformed.edgeIndex(1).value.get.data should contain theSameElementsAs Seq(11, 11)
    graphTransformed.edgeIndex(2).value.get.data should contain theSameElementsAs Seq(12, 12)
    graphTransformed.edgeIndex(3).value.get.data should contain theSameElementsAs Seq(13, 13)
  }

  "popDimension" should "throw an exception if there is dimension to pop" in {
    a[IllegalStateException] should be thrownBy {
      testGraphSimple.popDimension(
        vertices => vertices.map(_._3),
        edges => edges.map(_._3)
      )
    }
  }

  "numDimensions" should "return number of dimensions in the graph" in {
    testGraphSimple.numDimensions shouldEqual 1

    val graphTransformed = testGraphSimple.pushDimension(e => (1 to 2).map(_.toLong))
    graphTransformed.numDimensions shouldEqual 2

    graphTransformed.popDimension(
      vertices => vertices.map(_._3),
      edges => edges.map(_._3)
    ).numDimensions shouldEqual 1
  }

  "mapDimension" should "should apply mapFunc to each graph in the last dimension added" in {
    val graphTransformed = testGraphSimple.pushDimension(
      e => (1 to 2).map(_.toLong)
    ).mapDimension(
      g => {
        g.numDimensions shouldEqual 1

        g.mapVertices(
          _.data / g.vertices.map(_.data).sum.toDouble
        ).mapEdges(_.data / g.edges.map(_.data).sum.toDouble)
      }
    )

    graphTransformed.numDimensions shouldEqual 2

    graphTransformed.vertexIndex(1)(1).value.get.data shouldEqual (1 / 10.0)
    graphTransformed.vertexIndex(1)(2).value.get.data shouldEqual (2 / 10.0)
    graphTransformed.vertexIndex(1)(3).value.get.data shouldEqual (3 / 10.0)
    graphTransformed.vertexIndex(1)(4).value.get.data shouldEqual (4 / 10.0)

    graphTransformed.vertexIndex(2)(1).value.get.data shouldEqual (1 / 10.0)
    graphTransformed.vertexIndex(2)(2).value.get.data shouldEqual (2 / 10.0)
    graphTransformed.vertexIndex(2)(3).value.get.data shouldEqual (3 / 10.0)
    graphTransformed.vertexIndex(2)(4).value.get.data shouldEqual (4 / 10.0)


    graphTransformed.edgeIndex(1)(1).value.get.data shouldEqual (1 / 10.0)
    graphTransformed.edgeIndex(1)(2).value.get.data shouldEqual (2 / 10.0)
    graphTransformed.edgeIndex(1)(3).value.get.data shouldEqual (3 / 10.0)
    graphTransformed.edgeIndex(1)(4).value.get.data shouldEqual (4 / 10.0)

    graphTransformed.edgeIndex(2)(1).value.get.data shouldEqual (1 / 10.0)
    graphTransformed.edgeIndex(2)(2).value.get.data shouldEqual (2 / 10.0)
    graphTransformed.edgeIndex(2)(3).value.get.data shouldEqual (3 / 10.0)
    graphTransformed.edgeIndex(2)(4).value.get.data shouldEqual (4 / 10.0)
  }

  "reverseEdges" should "reverse the orientation of all edges in the graph" in {
    val graphTransformed = testGraphSimple.reverseEdges()

    graphTransformed.edgeIndex(1).value.get.srcId.head shouldEqual 2
    graphTransformed.edgeIndex(1).value.get.dstId.head shouldEqual 1

    graphTransformed.edgeIndex(2).value.get.srcId.head shouldEqual 3
    graphTransformed.edgeIndex(2).value.get.dstId.head shouldEqual 1

    graphTransformed.edgeIndex(3).value.get.srcId.head shouldEqual 4
    graphTransformed.edgeIndex(3).value.get.dstId.head shouldEqual 2

    graphTransformed.edgeIndex(4).value.get.srcId.head shouldEqual 4
    graphTransformed.edgeIndex(4).value.get.dstId.head shouldEqual 3
  }

  "degree" should "return a in-degree graph" in {
    val degreeGraph = testGraphSimple.degree()
    degreeGraph.vertexIndex(1).value.get.data shouldEqual 0
    degreeGraph.vertexIndex(2).value.get.data shouldEqual 1
    degreeGraph.vertexIndex(3).value.get.data shouldEqual 1
    degreeGraph.vertexIndex(4).value.get.data shouldEqual 2
  }
}
