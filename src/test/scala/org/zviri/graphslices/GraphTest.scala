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

  "triplets" should "return list of all (vertex, edge, vertex) triplets in the graph" in {
    testGraphSimple.triplets().map(
      triplet => (triplet.srcVertex.id, triplet.dstVertex.id, triplet.edge.id)
    ) should contain theSameElementsAs
      Seq((Seq(1), Seq(2), Seq(1)), (Seq(1), Seq(3), Seq(2)), (Seq(2), Seq(4), Seq(3)), (Seq(3), Seq(4), Seq(4)))
  }

  "mapTriplets" should "transform edge data providing access to from vertices" in {
    val graphTransformed = testGraphSimple.mapTriplets(triplet => triplet.edge.data / triplet.srcVertex.data.toDouble)
    graphTransformed.edgeIndex(1).value.get.data shouldEqual 1.0
    graphTransformed.edgeIndex(2).value.get.data shouldEqual 2.0
    graphTransformed.edgeIndex(3).value.get.data shouldEqual (3 / 2.0)
    graphTransformed.edgeIndex(4).value.get.data shouldEqual (4 / 3.0)
  }

  "subgraph" should "return a filtered subgraph based on the provided predicates" in {
    val transformedGraph = testGraphSimple.subgraph(
      triplet => triplet.edge.id != Seq(1), vertex => vertex.id != Seq(4)
    )

    transformedGraph.edges.map(_.id) should contain theSameElementsAs Seq(Seq(2))
    transformedGraph.vertices.map(_.id) should contain theSameElementsAs Seq(Seq(1), Seq(2), Seq(3))
  }

  "aggregateNeighbours" should "sends message via mapFunc over each edge (in-direction) and aggregates incoming edges via reduceFunc" in {
    val graphTransformed = testGraphSimple.aggregateNeighbors[Double](
      ctx => Seq(ctx.msgToDst(ctx.srcVertex.data * ctx.edge.data.toDouble)), (m1, m2) => m1 + m2
    )

    a[NoSuchElementException] should be thrownBy {
      graphTransformed.vertexIndex(1)
    }

    graphTransformed.vertexIndex(2).value.get.data shouldEqual 1.0
    graphTransformed.vertexIndex(3).value.get.data shouldEqual 2.0
    graphTransformed.vertexIndex(4).value.get.data shouldEqual 18.0
  }

  "aggregateNeighbours" should "sends message via mapFunc over each edge (against-direction) and aggregates incoming edges via reduceFunc" in {
    val graphTransformed = testGraphSimple.aggregateNeighbors[Double](
      ctx => Seq(ctx.msgToSrc(ctx.dstVertex.data * ctx.edge.data.toDouble)), (m1, m2) => m1 + m2
    )

    a[NoSuchElementException] should be thrownBy {
      graphTransformed.vertexIndex(4)
    }

    graphTransformed.vertexIndex(1).value.get.data shouldEqual 8.0
    graphTransformed.vertexIndex(2).value.get.data shouldEqual 12.0
    graphTransformed.vertexIndex(3).value.get.data shouldEqual 16.0
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
    val graphTransformed = testGraphSimple.pushDimension(e => (1 to 2).map(dim => (dim.toLong, e.data)))

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

  "pushDimension" should "should add a new dimension to the graph and keep all the nodes in every time window" in {
    val graphTransformed = Graph(Seq(
      Vertex(Seq(1), 1),
      Vertex(Seq(2), 2),
      Vertex(Seq(3), 3)
    ), Seq(
      Edge(Seq(1), Seq(1), Seq(2), 1),
      Edge(Seq(2), Seq(2), Seq(3), 2)
    )).pushDimension(e => Seq((e.data.toLong, e.data)), keepAllNodes = true)

    graphTransformed.vertexIndex(1)(1).value.get.data shouldEqual 1
    graphTransformed.vertexIndex(1)(2).value.get.data shouldEqual 2
    graphTransformed.vertexIndex(1)(3).value.get.data shouldEqual 3

    graphTransformed.vertexIndex(2)(1).value.get.data shouldEqual 1
    graphTransformed.vertexIndex(2)(2).value.get.data shouldEqual 2
    graphTransformed.vertexIndex(2)(3).value.get.data shouldEqual 3


    graphTransformed.edgeIndex(1)(1).value.get.data shouldEqual 1

    graphTransformed.edgeIndex(2)(2).value.get.data shouldEqual 2
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
    )).pushDimension(e => (1 to 2).map(dim => (dim.toLong, e.data)))

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

    val graphTransformed = testGraphSimple.pushDimension(e => (1 to 2).map(dim => (dim.toLong, e.data)))
    graphTransformed.numDimensions shouldEqual 2

    graphTransformed.popDimension(
      vertices => vertices.map(_._3),
      edges => edges.map(_._3)
    ).numDimensions shouldEqual 1
  }

  "mapDimension" should "should apply mapFunc to each graph in the last dimension added" in {
    val graphTransformed = testGraphSimple.pushDimension(
      e => (1 to 2).map(dim => (dim.toLong, e.data))
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
}
