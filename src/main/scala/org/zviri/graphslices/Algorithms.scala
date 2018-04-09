package org.zviri.graphslices

import scala.util.Random

object Algorithms {

  def inDegree[VD, ED](graph: Graph[VD, ED]): Graph[Int, ED] = {
    val degrees = graph.aggregateNeighbors[Int](ctx => Seq(ctx.msgToDst(1)), (a, b) => a + b).vertices.map(v => (v.id, v.data))
    graph.outerJoinVertices(degrees) {
      (v, d) => d.getOrElse(0)
    }
  }

  def outDegree[VD, ED](graph: Graph[VD, ED]): Graph[Int, ED] = {
    inDegree(graph.reverseEdges())
  }

  def degree[VD, ED](graph: Graph[VD, ED]): Graph[Int, ED] = {
    inDegree(graph).outerJoinVertices(outDegree(graph).vertices.map(v => (v.id, v.data))) {
      (vertex, outDegree) => vertex.data + outDegree.getOrElse(0)
    }
  }

  def inDegreeWeighted[VD](graph: Graph[VD, Double]): Graph[Double, Double] = {
    val degrees = graph.aggregateNeighbors[Double](ctx => Seq(ctx.msgToDst(ctx.edge.data)), (a, b) => a + b).vertices.map(v => (v.id, v.data))
    graph.outerJoinVertices(degrees) {
      (v, d) => d.getOrElse(0.0)
    }
  }

  def outDegreeWeighted[VD](graph: Graph[VD, Double]): Graph[Double, Double] = {
    inDegreeWeighted(graph.reverseEdges())
  }

  def pagerank[VD](graph: Graph[VD, Double], resetProb: Double = 0.15, numIter: Int = 100): Graph[Double, Double] = {
    val degrees = outDegreeWeighted(graph).vertices.map(v => (v.id, v.data))

    var i = 0
    var rankGraph = graph.outerJoinVertices(degrees) {
      (vertex, degree) => degree.getOrElse(0.0)
    }.mapTriplets(
      triplet => triplet.edge.data / triplet.srcVertex.data
    ).mapVertices[Double](_ => 1.0)

    while (i < numIter) {
      i += 1

      val rankUpdates = rankGraph.aggregateNeighbors[Double](
        edgeCtx => Seq(edgeCtx.msgToDst(edgeCtx.srcVertex.data * edgeCtx.edge.data)),
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

  def hits[VD](graph: Graph[VD, Double], numIter: Int = 100, normalizeEvery: Int = 5): Graph[HitsScore, Double] = {

    def normalizeRec(graph: Graph[Double, Double]): Graph[Double, Double] = {
      if (graph.numDimensions == 1) {
        val sum = graph.vertices.map(v => v.data).sum
        graph.mapVertices(v =>  v.data / sum)
      } else {
        graph.mapDimension(normalizeRec)
      }
    }

    var i = 1
    var hGraph = graph.mapVertices(_ => 1.0 / graph.vertices.size)
    var aGraph = hGraph.reverseEdges()
    while (i < numIter + 1) {
      val hGraphUpdates = hGraph.aggregateNeighbors[Double](
        edgeCtx => Seq(edgeCtx.msgToDst(edgeCtx.srcVertex.data * edgeCtx.edge.data)),
        (a, b) => a + b
      )
      aGraph = aGraph.outerJoinVertices(hGraphUpdates.vertices.map(v => (v.id, v.data))) {
        (vertex, hSum) => hSum.getOrElse(0.0)
      }

      val aGraphUpdates = aGraph.aggregateNeighbors[Double](
        edgeCtx => Seq(edgeCtx.msgToDst(edgeCtx.srcVertex.data * edgeCtx.edge.data)),
        (a, b) => a + b
      )
      hGraph = hGraph.outerJoinVertices(aGraphUpdates.vertices.map(v => (v.id, v.data))) {
        (vertex, aSum) => aSum.getOrElse(0.0)
      }

      if (i % normalizeEvery == 0) {
        hGraph = normalizeRec(hGraph)
        aGraph = normalizeRec(aGraph)
      }

      i += 1
    }

    hGraph.outerJoinVertices(aGraph.vertices.map(v => (v.id, v.data))) {
      (vertex, authority) => HitsScore(vertex.data, authority.getOrElse(0.0))
    }
  }

  def maxIndependentSet[VD, ED](graph: Graph[VD, ED], maxIter: Int = 30): Graph[Int, ED] = {

    object Status extends Enumeration {
      type Status = Value
      val Unknown, TentativelyInS, InS, NotInS = Value
    }
    import Status._

    case class MIS(var status: Status, var degree: Int, var active: Boolean)


    var graphIter = inDegree(graph).outerJoinVertices(outDegree(graph).vertices.map(v => (v.id, v.data))) {
      (inDegVertex, outDegVertex) => inDegVertex.data + outDegVertex.getOrElse(0)
    }.mapVertices(v => MIS(Unknown, v.data, active = true))

    while (graphIter.vertices.exists(v => v.data.status == Unknown)) {

      val tentativelyInS = graphIter.mapVertices(v => {
        if (v.data.status == Unknown) {
          if (v.data.degree == 0 || Random.nextFloat() <= 1.0 / (2 * v.data.degree))  v.data.status = TentativelyInS
        }
        v.data
      }).aggregateNeighbors[Seq[Id]](ctx => Seq(
          (ctx.srcVertex.data.status == TentativelyInS, ctx.msgToDst(Seq(ctx.srcVertex.id))),
          (ctx.dstVertex.data.status == TentativelyInS, ctx.msgToSrc(Seq(ctx.dstVertex.id)))
        ).filter(_._1).map(_._2), _ ++ _)
      graphIter = graphIter.outerJoinVertices(tentativelyInS.vertices.map(v => (v.id, v.data))) {
        (vertex, message) =>
          if (vertex.data.status == TentativelyInS) {
            if (vertex.id.last < message.getOrElse(Seq(Seq(Long.MaxValue))).map(_.last).min)
              vertex.data.status = InS
            else
              vertex.data.status = Unknown
          }
          vertex.data
      }

      val notInS = graphIter.aggregateNeighbors[Int](
        ctx => Seq(
            (ctx.srcVertex.data.status == InS, ctx.msgToDst(1)),
            (ctx.dstVertex.data.status == InS, ctx.msgToSrc(1))
          ).filter(_._1).map(_._2), _ & _)
      graphIter = graphIter.outerJoinVertices(notInS.vertices.map(v => (v.id, v.data))) {
        (vertex, message) => {
          if (message.isDefined) vertex.data.status = NotInS
          vertex.data
        }
      }

      val reduceDegree = graphIter.aggregateNeighbors[Int](ctx => Seq(
          (ctx.srcVertex.data.active && ctx.srcVertex.data.status == NotInS, ctx.msgToDst(1)),
          (ctx.dstVertex.data.active && ctx.dstVertex.data.status == NotInS, ctx.msgToSrc(1))
        ).filter(_._1).map(_._2), _ + _)
      graphIter = graphIter.outerJoinVertices(reduceDegree.vertices.map(v => (v.id, v.data))) {
        (vertex, message) => {
          if (vertex.data.status == Unknown && message.isDefined) vertex.data.degree -= message.get
          if (vertex.data.status == NotInS && vertex.data.active) vertex.data.active = false
          vertex.data
        }
      }
    }

    graphIter.mapVertices(v => if (v.data.status == InS) 1 else 0)
  }

  def greedyColor[VD, ED](graph: Graph[VD, ED], maxIter: Int = 30): Graph[Int, ED] = {
    var graphIter = graph.mapVertices(_ => 0)
    var color = 0
    var newVertices = Seq[Vertex[Int]]()

    while (graphIter.vertices.nonEmpty) {
      val indSet = maxIndependentSet(graphIter)
      newVertices ++= indSet.vertices.filter(v => v.data == 1).map(v => Vertex(v.id, color))
      graphIter = indSet.subgraph(vertexPredicate = vertex => vertex.data == 0)
      color += 1
    }

    Graph(newVertices, graph.edges)
  }

  def clusterLPA[VD, ED](graph: Graph[VD, ED]): Graph[Int, ED] = {

    val coloredGraph = greedyColor(graph)

    case class LPA(color: Int, var cluster: Int)

    val lpaVertices: Seq[Vertex[LPA]] = coloredGraph.vertices.zipWithIndex.map { case (v, idx) => Vertex(v.id, LPA(v.data, idx)) }
    var graphIter = Graph(lpaVertices, graph.edges)
    val maxColor = coloredGraph.vertices.map(v => v.data).max

    var isUpdate = true

    while (isUpdate) {
      Range(0, maxColor + 1).foreach { color =>
          val clusterUpdates = graphIter.aggregateNeighbors[Seq[Int]](
            ctx => Seq(
              (ctx.dstVertex.data.color == color, ctx.msgToDst(Seq(ctx.srcVertex.data.cluster))),
              (ctx.srcVertex.data.color == color, ctx.msgToSrc(Seq(ctx.dstVertex.data.cluster)))
            ).filter(_._1).map(_._2), _ ++ _
          ).vertices.filter(v => v.data.nonEmpty).map(
            v => {
              (v.id, v.data.groupBy(c => c).toSeq.map {
                case (group, values) => (values.size, group)
              }.max._2)
            }
          )

          val graphNewClusters = graphIter.outerJoinVertices(clusterUpdates) {
            (vertex, clusterUpdate) =>
              val newCluster = clusterUpdate.getOrElse(vertex.data.cluster)
              val updated = newCluster != vertex.data.cluster
              vertex.data.cluster = newCluster
              (vertex.data, updated)
          }

          graphIter = graphNewClusters.mapVertices(v => v.data._1)
          isUpdate = graphNewClusters.vertices.exists(v => v.data._2)
      }
    }

    val reindexClustersMap = graphIter.vertices.map(v => v.data.cluster).distinct.sorted.zipWithIndex.toMap
    graphIter.mapVertices(v => reindexClustersMap(v.data.cluster))
  }
}
