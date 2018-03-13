package org.zviri.graphslices

import sun.security.provider.certpath.Vertex

import scala.io.Source

object Main {
  case class NodeIsir(id: Long, name: String, nodeType: String)
  case class EdgeIsir(sourceId: Long, targetId: Long, source: String, target: String, edgeType: String, year: Int)


  def main(args: Array[String]): Unit = {
    val data_dir = "/Users/peterzvirinsky/remotes/dev1-zviri-cz/data/isir/networks_2.0"

    val nodesIsir = List.range(2008, 2016).flatMap {
      year =>
        val nodesIsir = Source.fromFile(
          s"$data_dir/nodes_Ústeckýkraj_$year.tsv"
        ).getLines().drop(1).map(
          row => row.trim.split("\t", 3)
        ).map {
          case Array(id, name, nodeType) => NodeIsir(id.hashCode, name, nodeType)
        }.toVector
        nodesIsir
    }.distinct

    val nodesSet = nodesIsir.map(_.id).toSet
    val edgesIsir = List.range(2008, 2016).flatMap {
      year =>
        val edges = Source.fromFile(
          s"$data_dir/edges_Ústeckýkraj_$year.tsv"
        ).getLines().drop(1).map(
          row => row.trim.split("\t", 3)
        ).map {
          case Array(sourceId, targetId, edgeType) => EdgeIsir(sourceId.hashCode, targetId.hashCode, sourceId, targetId, edgeType, year)
        }.toVector
        edges
    }.filter(
      e => nodesSet.contains(e.sourceId) && nodesSet.contains(e.targetId)
    ).groupBy(
      e => (e.sourceId, e.targetId)
    ).map {
      case (key, grp) => grp.head
    }

    val vertices = nodesIsir.map(n => Vertex[Double](Seq(n.id), 1.0 / nodesIsir.length)).toVector
    val edges = edgesIsir.zipWithIndex.map {
      case (e, idx) => Edge(Seq(idx), Seq(e.sourceId), Seq(e.targetId), e.year)
    }.toVector
    val graph = Graph(vertices, edges).pushDimension(e => Seq(e.data))

    val graphPr = Algorithms.hits(graph, numIter = 100)
    val graphPrMap = graphPr.vertices.map(v => (v.id, v.data)).toMap
    for (node <- nodesIsir.drop(1).take(100)) {
      println(node.nodeType)
      for (year <- List.range(2008, 2016)) {
        val v = graphPrMap.get(Seq(year, node.id))
        println(s"\t$year: $v")
      }
    }
  }


}
