package com.corey.algorithms.spark

import com.corey.algorithms.GraphBuilder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable.Buffer

object GraphXGraphBuilder {
  def buildGraphXCoopCompanyGraph(filename: String): Graph[String, Int]= {
	  val coopCompanyGraph: GraphBuilder.CoopCompanyGraph = GraphBuilder.buildCoopCompanyGraph(filename) 
    
    // A map of companies to forwardly-linked companies and the total weight
    // of forward links to each company, represented as a tuple in the form of
    // (company, weight)
    var weightedLinksSeq: Seq[(String, String, Int)] = Seq()
    
    for (company <- coopCompanyGraph.getCompanyMap().values) {
      for (forwardCompanyLinks: (String, Buffer[GraphBuilder.Link]) <- company.forwardLinks) {
        for (forwardLink: GraphBuilder.Link <- forwardCompanyLinks._2) {
          weightedLinksSeq +:= (forwardLink.firstCompanyName, forwardLink.secondCompanyName, forwardLink.weight)
        }
      }
    }
    
    val conf = new SparkConf().setAppName("Page Rank Algorithm").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    
    // Create Vertex RDD
    val vertices: RDD[(VertexId, String)] = sc.parallelize(coopCompanyGraph.getCompanyMap().map {
      case (companyName, company) => (companyName.hashCode().toLong, companyName)
    }.toSeq)
    
    // Create Edge RDD
    val edges: RDD[Edge[Int]] = sc.parallelize(weightedLinksSeq).map {
      case (firstCompanyName, secondCompanyName, weight) => {
        val vertexId1: VertexId = firstCompanyName.hashCode.toLong
        val vertexId2: VertexId = secondCompanyName.hashCode.toLong
        
        Edge(math.min(vertexId1, vertexId2), math.max(vertexId1, vertexId2), weight)
      }
    }
    
    val graph = Graph(vertices, edges)
    graph
  }
  
}